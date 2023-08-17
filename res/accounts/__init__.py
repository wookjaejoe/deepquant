import pandas as pd
import os

__folder = os.path.dirname(__file__)


def load_one(name):
    return pd.read_csv(os.path.join(__folder, name))


__filenames = ["bs.csv", "is.csv", "cis.csv", "cf.csv"]


def accounts():
    return pd.concat([load_one(filename) for filename in __filenames])[["concept_id", "label_ko"]]
