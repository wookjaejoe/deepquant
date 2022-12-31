import os
import json

_config_file = os.path.join(os.path.dirname(__file__), os.pardir, ".config.json")

assert os.path.isfile(_config_file), f"The file {_config_file} not found"

with open(_config_file) as f:
    config = json.load(f)
