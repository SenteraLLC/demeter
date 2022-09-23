from typing import Callable, Dict, List


from io import TextIOWrapper
import os

this_dir = os.path.dirname(__file__)
open_sql : Callable[[str], TextIOWrapper] = lambda filename : open(os.path.join(this_dir, filename))

get_features_stmt = open_sql('get_features.sql').read()
get_action_planting_stmt = open_sql('get_action_planting.sql').read()

import psycopg2.extras

import json

import pandas

from typing import Any
from collections import OrderedDict


if __name__ == '__main__':
  connection = psycopg2.connect(host="localhost", database="postgres", options="-c search_path=test_mlops,public")

  #connection = demeter.db.getConnection()

  root_id = 60986
  whitelist = { 68, 80, 64, 65, 81  }
  #whitelist = { 68, 80 }
  #whitelist = set()

  cursor = connection.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
  args = {"field_group_id": root_id, "local_type_id_whitelist": list(whitelist) }
  cursor.execute(get_features_stmt, args)

  feature_results = cursor.fetchall()
  print("FEATURE RESULTS: ",len(feature_results))
  print("FIRST: ",feature_results[0])
  #import sys
  #sys.exit(1)

  field_ids = set()
  for r in feature_results:
    field_ids.add(r["columns"]["field_id"])
  args = {"field_ids": list(field_ids) }
  cursor.execute(get_action_planting_stmt, args)
  field_results = cursor.fetchall()
  print("FIELD RESULTS: ",len(field_results))

  field_id_to_columns : OrderedDict[int, List[Dict[str, Any]]] = OrderedDict()

  id_to_columns : OrderedDict[int, OrderedDict[int, Any]] = OrderedDict()
  for r in feature_results:
    columns_to_keep = {'unit_name', 'acquired', 'local_type_name', 'local_type_id', 'unit_type_id'}

    l = r["local_value_id"]

    c = r["columns"]
    f = c["field_id"]
    try:
      field_id_to_columns[f]
    except KeyError:
      field_id_to_columns[f] = []
    field_id_to_columns[f].append(c)

  columns_out = open("/tmp/columns.json", "w")
  json.dump(list(id_to_columns.values()), columns_out, indent=2)

  id_to_field : OrderedDict[int, OrderedDict[int, Any]] = OrderedDict()
  for r in field_results:
    f = r["field_id"]
    m = r["field_meta"]
    id_to_field[f] = m
    try:
      if (a := m["actions"]):
        field_id_to_columns[f].append(a)
    except KeyError:
      pass

    try:
      if (p := m["plantings"]):
        field_id_to_columns[f].append(p)
    except KeyError:
      pass

  fields_out = open("/tmp/fields.json", "w")
  json.dump(id_to_field, fields_out, indent=2)

  field_to_features_out = open("/tmp/field_to_features.json", "w")
  json.dump(field_id_to_columns, field_to_features_out, indent=2)

  print("Done.")

