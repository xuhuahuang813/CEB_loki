import json
import traceback


config = {}


def load_config(config_file):
    global config
    with open(config_file, 'r') as f:
        config = json.load(f)
