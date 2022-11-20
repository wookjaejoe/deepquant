import os

cache_dir = os.path.join(os.path.dirname(__file__), os.path.pardir, ".cache")
os.makedirs(cache_dir, exist_ok=True)


def cache_file(*subpath: str):
    return os.path.join(cache_dir, *subpath)
