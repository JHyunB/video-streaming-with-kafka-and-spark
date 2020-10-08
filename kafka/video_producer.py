from kafka import KafkaProducer
import cv2


topic = "testing"
producer = KafkaProducer(bootstrap_servers='localhost:9092')

video_file = "test_data/Robbery056_x264.mp4"
video = cv2.VideoCapture(video_file)

while video.isOpened():
    success, frame = video.read()
    if not success:
        break
    _, img = cv2.imencode('.jpg', frame)

    producer.send(topic, img.tobytes())

video.release()