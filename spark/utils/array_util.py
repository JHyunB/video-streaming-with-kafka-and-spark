import numpy as np


def chunks(l, n):
    for i in range(0, len(l), n):
        yield l[i:i + n]


def interpolate(features, features_per_bag):
    # 기존 172, 4096
    feature_size = np.array(features).shape[1]
    # 32, 4096
    interpolated_features = np.zeros((features_per_bag, feature_size))
    interpolation_indicies = np.round(np.linspace(0, len(features) - 1, num=features_per_bag + 1))
    count = 0
    for index in range(0, len(interpolation_indicies) - 1):
        start = int(interpolation_indicies[index])
        end = int(interpolation_indicies[index + 1])

        assert end >= start

        if start == end:
            temp_vect = features[start, :]
        else:
            temp_vect = np.mean(features[start:end + 1, :], axis=0)

        temp_vect = temp_vect / np.linalg.norm(temp_vect)

        if np.linalg.norm(temp_vect) == 0:
            print("Error")

        interpolated_features[count, :] = temp_vect
        count = count + 1

    return np.array(interpolated_features)


def extrapolate(outputs, num_frames):
    extrapolated_outputs = []
    extrapolation_indicies = np.round(np.linspace(0, len(outputs) - 1, num=num_frames))
    for index in extrapolation_indicies:
        extrapolated_outputs.append(outputs[int(index)])
    return np.array(extrapolated_outputs)
