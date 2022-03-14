import json
import os

from typing import List, Any

from ..lib import types
from ..lib import schema_api

my_path = os.path.dirname(os.path.realpath(__file__))
GEO_KEYS = json.load(open(os.path.join(my_path, "../scripts/test_data/geospatial.json")))
TEMPORAL_KEYS = json.load(open(os.path.join(my_path, "../scripts/test_data/temporal.json")))



