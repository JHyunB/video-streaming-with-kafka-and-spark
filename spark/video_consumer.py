import sys
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils

import cv2
import numpy as np

import spark.parameters as params
from spark.c3d import *
from spark.classifier import *

from spark.utils.array_util import *


def deserializer(img):
    return img[0], np.frombuffer(img[1], dtype=np.uint8)


def decode(img):
    return img[0], cv2.cvtColor(cv2.imdecode(img[1], cv2.IMREAD_COLOR), cv2.COLOR_BGR2RGB)


def sliding_window(arr, size, stride):
    num_chunks = int((len(arr) - size) / stride) + 2
    result = []
    for i in range(0, num_chunks * stride, stride):
        if len(arr[i:i + size]) > 0:
            result.append(arr[i:i + size])
    return np.array(result)


sc = SparkContext(appName="test")
ssc = StreamingContext(sc,1)
brokers, topic = sys.argv[1:]
kafka_stream = KafkaUtils.createDirectStream(ssc,[topic],{"metadata.broker.list":brokers}, valueDecoder=lambda x: x)
frames = kafka_stream.map(deserializer).map(decode).map(lambda x: x[1])
frame_list = []
frames.foreachRDD(lambda x:frame_list.append(x.collect())) # rdd -> list
video_clips = sliding_window(frame_list, params.frame_count, params.frame_count) # list -> np

# build models
feature_extractor = c3d_feature_extractor()
classifier_model = build_classifier_model()

# extract features
rgb_features = []
# 16프레임 단위로 특징 추출
for i, clip in enumerate(video_clips):
    # list -> array
    clip = np.array(clip)
    # 마지막 클립의 경우 처리 스킵
    if len(clip) < params.frame_count:
        continue

    clip = preprocess_input(clip)
    sc.parallelize(clip)
    rgb_feature = feature_extractor.predict(clip)[0]
    rgb_features.append(rgb_feature)


rgb_features = np.array(rgb_features) # list -> np

# bag features
rgb_feature_bag = interpolate(rgb_features, params.features_per_bag)

# classify using the trained classifier model
sc.parallelize(rgb_feature_bag)
predictions = classifier_model.predict(rgb_feature_bag)
predictions = np.array(predictions).squeeze()

# predictions
predictions = extrapolate(predictions,len(frame_list))
frames.pprint()
ssc.start()
ssc.awaitTermination()
