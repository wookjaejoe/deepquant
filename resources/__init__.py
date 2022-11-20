import os


def get_resource(*subpath: str) -> str:
    return os.path.join(os.path.dirname(__file__), *subpath)
