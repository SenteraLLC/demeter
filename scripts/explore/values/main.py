import argparse
import json
import os
from collections import OrderedDict
from io import TextIOWrapper
from typing import Any, Callable, Dict, List, Set

import psycopg2.extras
from dotenv import load_dotenv

import demeter.db
from demeter.db import TableId

this_dir = os.path.dirname(__file__)
open_sql: Callable[[str], TextIOWrapper] = lambda filename: open(os.path.join(this_dir, filename))

get_features_stmt = open_sql("get_features.sql").read()
get_action_planting_stmt = open_sql("get_action_planting.sql").read()


def main(
    cursor: Any,
    field_ids: Set[TableId],
    observation_type_ids: Set[TableId],
) -> None:
    args = {
        "field_ids": list(field_ids),
        "observation_type_ids": list(observation_type_ids),
    }
    print("ARGS: ", args)
    cursor.execute(get_features_stmt, args)

    feature_results = cursor.fetchall()
    print("FEATURE RESULTS: ", len(feature_results))
    print("FIRST: ", feature_results[0])

    field_ids = set()
    for r in feature_results:
        # print("R IS: ",r)
        field_ids.add(r.column["field_id"])
    args = {"field_ids": list(field_ids)}
    # print("ARGS: ",args)
    cursor.execute(get_action_planting_stmt, args)
    field_results = cursor.fetchall()
    print("FIELD RESULTS: ", len(field_results))

    field_id_to_columns: OrderedDict[int, List[Dict[str, Any]]] = OrderedDict()

    name_to_column: OrderedDict[int, OrderedDict[int, Any]] = OrderedDict()
    for r in feature_results:
        _ = {
            "unit_name",
            "acquired",
            "observation_type_name",
            "observation_type_id",
            "unit_type_id",
        }

        _ = r.observation_value_id

        c = r.column
        f = c["field_id"]
        try:
            field_id_to_columns[f]
        except KeyError:
            field_id_to_columns[f] = []
        field_id_to_columns[f].append(c)
        name_to_column[c["internal_ids"]["column_name"]] = c["quantity"]

    columns_out = open("/tmp/columns.json", "w")
    json.dump(list(name_to_column.items()), columns_out, indent=2)

    id_to_field: OrderedDict[int, OrderedDict[int, Any]] = OrderedDict()
    for r in field_results:
        f = r.field_id
        m = r.field_meta
        id_to_field[f] = m
        try:
            if a := m["actions"]:
                field_id_to_columns[f].append(a)
        except KeyError:
            pass

        try:
            if p := m["plantings"]:
                field_id_to_columns[f].append(p)
        except KeyError:
            pass

    fields_out = open("/tmp/fields.json", "w")
    json.dump(id_to_field, fields_out, indent=2)

    field_to_features_out = open("/tmp/field_to_features.json", "w")
    json.dump(field_id_to_columns, field_to_features_out, indent=2)


if __name__ == "__main__":
    load_dotenv()

    parser = argparse.ArgumentParser(description="Load a field and all of its input data as JSON")

    parser.add_argument("--field_ids", type=int, nargs="+", help="list of field ids")
    parser.add_argument(
        "--observation_type_ids",
        type=int,
        nargs="+",
        help="list of observation type ids",
    )
    args = parser.parse_args()

    # connection = psycopg2.connect(host="observationhost", database="postgres", options="-c search_path=test_demeter,public")
    connection = demeter.db.getConnection()

    field_ids = set(args.field_ids)
    observation_ids = set(args.observation_type_ids)

    # root_id = 60986
    # whitelist = { 68, 80, 64, 65, 81 }
    # whitelist = { 68, 80 }
    # whitelist = set()

    cursor = connection.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    main(cursor, field_ids, observation_ids)

    print("Done.")
