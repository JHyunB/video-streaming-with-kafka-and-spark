import sys
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils

import cv2
import numpy as np


def deserializer(img):
    return img[0], np.frombuffer(img[1], dtype=np.uint8)


def decode(img):
    return img[0], cv2.imdecode(img[1], cv2.IMREAD_COLOR)


sc = SparkContext(appName="test")
ssc = StreamingContext(sc,1)
brokers, topic = sys.argv[1:]
kafka_stream = KafkaUtils.createDirectStream(ssc,[topic],{"metadata.broker.list":brokers}, valueDecoder=lambda x: x)
frame = kafka_stream.map(deserializer).map(decode)
frame.pprint()
ssc.start()
ssc.awaitTermination()
