import sys
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from confluent_kafka.avro.serializer.message_serializer import MessageSerializer

import cv2
import numpy as np


def deserializer(img):
    return np.frombuffer(img, dtype=np.unit8)


def decode(img):
    return cv2.imdecode(img, cv2.IMREAD_COLOR)


sc = SparkContext(appName="test")
ssc = StreamingContext(sc,1)
brokers, topic = sys.argv[1:]
kafka_stream = KafkaUtils.createDirectStream(ssc,[topic],{"metadata.broker.list":brokers}, valueDecoder=MessageSerializer.decode_message)
frame = kafka_stream.map(deserializer).map(decode)
frame.pprint()
ssc.start()
ssc.awaitTermination()