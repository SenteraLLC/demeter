from typing import Callable
from io import TextIOWrapper

import os.path

this_dir = os.path.dirname(__file__)
open_sql : Callable[[str], TextIOWrapper] = lambda filename : open(os.path.join(this_dir, filename))

stmt = open_sql('getLatestFunctionSignature.sql')

