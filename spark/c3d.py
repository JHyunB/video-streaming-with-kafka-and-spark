# -*- coding: utf-8 -*-

import numpy as np
from keras.utils.data_utils import get_file
from scipy.misc import imresize

from bigdl.nn.layer import *


C3D_MEAN_PATH = 'https://github.com/adamcasson/c3d/releases/download/v0.1/c3d_mean.npy'


def preprocess_input(video):
    intervals = np.ceil(np.linspace(0, video.shape[0] - 1, 16)).astype(int)
    frames = video[intervals]

    # Reshape to 128x171
    reshape_frames = np.zeros((frames.shape[0], 128, 171, frames.shape[3]))
    for i, img in enumerate(frames):
        img = imresize(img, (128, 171), 'bicubic')
        reshape_frames[i, :, :, :] = img

    mean_path = get_file('c3d_mean.npy',
                         C3D_MEAN_PATH,
                         cache_subdir='models',
                         md5_hash='08a07d9761e76097985124d9e8b2fe34')

    mean = np.load(mean_path)
    reshape_frames -= mean
    # Crop to 112x112
    reshape_frames = reshape_frames[:, 8:120, 30:142, :]
    # Add extra dimension for samples
    reshape_frames = np.expand_dims(reshape_frames, axis=0)

    return reshape_frames



def c3d_feature_extractor():
    feature_extractor_model = Model.load_keras(json_path='./c3d.json')
    return feature_extractor_model
