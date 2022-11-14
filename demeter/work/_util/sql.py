import os.path

from io import TextIOWrapper


def openRelative(path: str, file: str) -> TextIOWrapper:
    return open(os.path.join(os.path.dirname(file), "../sql", path))
