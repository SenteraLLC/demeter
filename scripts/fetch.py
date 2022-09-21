stmt = """
    with recursive ancestry as (
      select parent_field_group_id,
             field_group_id,
             0 as depth
      from test_mlops.field_group
      where field_group_id = %(field_group_id)s
      UNION ALL
      select F.parent_field_group_id,
             F.field_group_id,
             A.depth + 1
      from ancestry A
      join test_mlops.field_group F on F.parent_field_group_id = A.field_group_id

   ), leaf as (
     select A.parent_field_group_id, A.field_group_id, F.field_id
     from ancestry A
     inner join test_mlops.field F on A.field_group_id = F.field_group_id
     where not exists (select * from test_mlops.field_group F where A.field_group_id = F.parent_field_group_id)

   ), type_features as (
     select L.field_id,
            LT.type_name,
            LT.type_category,
            LT.local_type_id,
            U.unit as unit_name,
            U.unit_type_id,
            LV.acquired,
            LV.quantity,
            LG.group_name
     from leaf L
     inner join
       (test_mlops.local_value LV
        inner join test_mlops.unit_type U on U.unit_type_id = LV.unit_type_id
        left join test_mlops.local_type LT on LT.local_type_id = U.local_type_id
        left join test_mlops.local_group LG on LG.local_group_id = LV.local_group_id
        left join test_mlops.geom LVG on LV.geom_id = LVG.geom_id
       ) on LV.field_id = L.field_id
     where array_length(%(local_type_id_whitelist)s::bigint[], 1) = 0 or LT.local_type_id = any(%(local_type_id_whitelist)s::bigint[])
  ), time_id as (
    select local_type_id,
           unit_type_id,
           acquired,
           row_number() over (partition by local_type_id, unit_type_id order by acquired asc) as time_id
    from type_features
    group by local_type_id, unit_type_id, acquired
    order by local_type_id, unit_type_id

  ), unit_id as (
    select local_type_id,
           unit_type_id,
           row_number() over () as unit_id
    from time_id
    group by local_type_id, unit_type_id

  ), feature_ids as (
    select F.field_id,
           T.local_type_id,
           T.unit_type_id,
           T.time_id,
           U.unit_id,
           F.quantity,
           row_number() over (partition by F.field_id, T.local_type_id, U.unit_type_id, F.quantity
                              order by T.acquired asc
                              RANGE BETWEEN INTERVAL '18 hours' PRECEDING AND INTERVAL '18 hours' FOLLOWING
                             ) as dupe_id
    from type_features F
    join time_id T on T.local_type_id = F.local_type_id and
                      T.unit_type_id = F.unit_type_id and
                      T.acquired = F.acquired
    join unit_id U on U.local_type_id = F.local_type_id and
                      U.unit_type_id = F.unit_type_id

  ), feature as (
    select F.field_id,
           F.local_type_id as feature_local_type_id,
           I.unit_id as feature_unit_id,
           I.time_id,
           concat_ws('.', F.field_id, F.type_name, F.unit_name, I.time_id) as prefix,
           F.unit_type_id,
           F.quantity,
           F.acquired
    from type_features F
    join feature_ids I on I.field_id = F.field_id and
                          I.local_type_id = F.local_type_id and
                          I.unit_type_id = F.unit_type_id and
                          I.quantity = F.quantity
    where I.dupe_id = 1

  ), dynamic_numeric_columns as (
    select T.field_id,
           T.local_type_id,
           F.time_id,
           F.unit_type_id,
           T.unit_name,
           T.type_name,
           F.acquired,
           jsonb_object_agg(
             C.column_name,
             C.column_value
           ) as columns
    from type_features T
      join feature F on F.field_id = T.field_id and
                        F.feature_local_type_id = T.local_type_id and
                        F.unit_type_id = T.unit_type_id and
                        F.quantity = T.quantity,
      lateral (
         select (F.prefix || '::quantity') as column_name,
         T.quantity as column_value
      ) as C
    group by T.field_id, T.local_type_id, F.unit_type_id, F.time_id, T.unit_name, T.type_name, F.acquired
    order by T.field_id, T.local_type_id, F.unit_type_id, F.time_id, T.unit_name, T.type_name, F.acquired

  ), field_actions as (
    select F.field_id,
           A.name,
           jsonb_agg(A.performed) as performed
    from feature F
    left join act A on F.field_id = A.field_id
    group by F.field_id, A.name

  ) select F.field_id as field_id,
           N.local_type_id,
           N.time_id,
           N.unit_type_id,
           F.details->'AlmanacSenteraId' as sentera_id,
           A.name as action_name,
           A.performed as action_performed,
           (jsonb_build_object(
             'unit', N.unit_name,
             'acquired', N.acquired,
             'field_id', F.field_id,
             'field_name', F.name,
             'field_external_id', F.external_id,
             'farm_name', FARM.name,
             'region_name', REGION.name
           ) || N.columns) as columns
    from dynamic_numeric_columns N
    join field F on F.field_id = N.field_id
    join field_actions A on F.field_id = A.field_id
    join field_group FARM on F.field_group_id = FARM.field_group_id
    join field_group REGION on FARM.parent_field_group_id = REGION.field_group_id
"""

import pandas

import psycopg2.extras

import json

import pandas

from typing import Any
from collections import OrderedDict

from psycopg2.extensions import register_adapter, AsIs

if __name__ == '__main__':
  connection = psycopg2.connect(host="localhost", database="postgres", options="-c search_path=test_mlops,public")

  #connection = demeter.db.getConnection()

  root_id = 60986
  whitelist = { 68 }

  #register_adapter(Set, lambda v : psycopg2.extensions.AsIs("".join(["'", v.name, "'"])))

  cursor = connection.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
  args = {"field_group_id": root_id, "local_type_id_whitelist": list(whitelist) }
  print("ARGS: ",args)
  cursor.execute(stmt, args)

  results = cursor.fetchall()
  print("NUM RESULTS: ",len(results))

  debug = False
  #debug = True
  import sys
  if debug:
    for i, r in enumerate(results):
      print(r)
      if i > 200:
        sys.exit(1)
    sys.exit(1)
  id_to_columns : OrderedDict[int, OrderedDict[int, Any]] = OrderedDict()
  for r in results:
    f = r["field_id"]
    c = r["columns"]
    if (si := r["sentera_id"]) is not None:
      c["sentera_id"] = si
    if (an := r["action_name"]) is not None:
      c[an] = r["action_performed"]
    try:
      id_to_columns[f]
    except KeyError:
      id_to_columns[f] = OrderedDict()
    id_to_columns[f].update(c)

  columns = list(id_to_columns.values())

  f = open("/tmp/features.json", "w")
  json.dump(columns, f, indent=2)
  print("Features written.")
  import sys
  sys.exit(1)

  df = pandas.DataFrame.from_records(columns, index='field_id')
  print("Dataframe created: ",df)

  g = open("/tmp/fields.json", "w")
  json.dump(df.to_dict(), g, indent=2)

  df.to_csv("/tmp/fields.csv")


  print("Done.")


