import cv2

import parameters as params
from utils.array_util import *


def get_video_clips(video_path):
    # frames 모든 프레임 들어있음
    frames = get_video_frames(video_path)
    # clips 16프레임 단위로 묶인 ndarray가 됨
    clips = sliding_window(frames, params.frame_count, params.frame_count)
    return clips, len(frames)


def get_video_frames(video_path):
    cap = cv2.VideoCapture(video_path)
    frames = []
    while (cap.isOpened()):
        ret, frame = cap.read()
        if ret == True:
            frames.append(cv2.cvtColor(frame, cv2.COLOR_BGR2RGB))
        else:
            break
    return frames
