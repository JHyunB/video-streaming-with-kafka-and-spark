import sys
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils

import cv2
import numpy as np

from spark.utils.array_util import *
import spark.parameters as params

from spark.utils.array_util import sliding_window


def deserializer(img):
    return img[0], np.frombuffer(img[1], dtype=np.unit8)


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
kafka_stream = KafkaUtils.createDirectStream(ssc,[topic],{"metadata.broker.list":brokers}, valueDecoder=MessageSerializer.decode_message)
frames = kafka_stream.map(deserializer).map(decode).map(lambda x: x[1])
clips = sliding_window(frames, params.frame_count, params.frame_count)

clips.pprint()
ssc.start()
ssc.awaitTermination()
