import os.path

from io import TextIOWrapper

def openRelative(path : str) -> TextIOWrapper:
  return open(os.path.join(os.path.dirname(__file__), "../sql", path))


