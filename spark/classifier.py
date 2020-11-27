from bigdl.nn.layer import *

def build_classifier_model():
    model = Model.load_keras(json_path='classifier.json')
    model.summary()
