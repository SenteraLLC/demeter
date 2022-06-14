from typing import Tuple, List, Any, Optional, Union

import sys

from psycopg2 import sql
from psycopg2.extensions import register_adapter, adapt
import psycopg2.extras

from .database.types_protocols import TableId
from .database.generators import generateInsertMany, PGJoin, PGFormat

from .types.execution import Key

from .types.inputs import HTTPVerb, HTTPType, S3ObjectKey, S3Object


def stringToHTTPVerb(s : str) -> HTTPVerb:
  return HTTPVerb[s.upper()]

def getHTTPByName(cursor : Any, http_type_name : str) -> Tuple[TableId, HTTPType]:
  stmt = """select * from http_type where type_name = %(name)s"""
  cursor.execute(stmt, { 'name' : http_type_name })

  results = cursor.fetchall()
  if len(results) != 1:
    raise Exception(f"HTTP Type does exist for {http_type_name}")
  result_with_id = results[0]

  http_type_id = result_with_id.http_type_id
  result = {k : (stringToHTTPVerb(v) if k == "verb" else v)
            for k, v in result_with_id._asdict().items()
            if k != 'http_type_id'
           }
  http_type = HTTPType(**result)

  return http_type_id, http_type


def insertS3ObjectKeys(cursor       : Any,
                       s3_object_id : TableId,
                       keys         : List[Key],
                       s3_type_id   : TableId,
                      ) -> bool:
  s3_key_names = S3ObjectKey.names()
  stmt = generateInsertMany("s3_object_key", s3_key_names, len(keys))
  args : List[int] = []
  for k in keys:
    s3_object_key = S3ObjectKey(
                      s3_object_id      = s3_object_id,
                      geospatial_key_id = k.geospatial_key_id,
                      temporal_key_id   = k.temporal_key_id,
                    )
    args.extend(s3_object_key().values())
  results = cursor.execute(stmt, args)
  return True


def getS3ObjectByKey(cursor         : Any,
                     key            : Key,
                    ) -> Optional[Tuple[TableId, S3Object]]:
  stmt = """select *
            from s3_object O, s3_object_key K
            where O.s3_object_id = K.s3_object_id and
                  K.geospatial_key_id = %(geospatial_key_id)s and
                  K.temporal_key_id = %(temporal_key_id)s
         """
  args = { k : v for k, v in key().items() if k in ["geospatial_key_id", "temporal_key_id"] }
  cursor.execute(stmt, args)
  results = cursor.fetchall()
  if len(results) <= 0:
    raise Exception("No S3 object exists for: ",key)

  result_with_id = results[0]
  s3_object_id = result_with_id.pop("s3_object_id")
  s3_object = result_with_id
  s = S3Object(**result_with_id)

  return s3_object_id, s


def getS3ObjectByKeys(cursor    : Any,
                      keys      : List[Key],
                      type_name : str,
                     ) -> Optional[Tuple[TableId, S3Object]]:
  clauses = []
  args : List[Union[str, int]] = [type_name]
  for k in keys:
    clauses.append(PGJoin("",
                          [sql.SQL("("),
                           PGJoin(" and ",
                                  [(sql.Placeholder()
                                    +
                                    sql.SQL(" = K.geospatial_key_id")
                                   ),
                                   (sql.Placeholder()
                                    +
                                    sql.SQL(" = K.temporal_key_id")
                                   )
                                  ]
                                 ),
                           sql.SQL(")"),
                          ]
                         )
                  )
    args.append(k.geospatial_key_id)
    args.append(k.temporal_key_id)

  filters = PGJoin(" or ", clauses)

  stmt = PGFormat("""select O.s3_object_id, O.key, O.bucket_name, O.s3_type_id, count(*)
                    from s3_object O
                    join s3_type T on T.type_name = %s and T.s3_type_id = O.s3_type_id
                    join s3_object_key K on O.s3_object_id = K.s3_object_id
                    where {filters}
                    group by O.s3_object_id, O.key, O.bucket_name, O.s3_type_id
                    order by O.s3_object_id desc
                 """, filters = filters)

  cursor.execute(stmt, args)
  results = cursor.fetchall()
  if len(results) <= 0:
    sys.stderr.write(f"No S3 object found for {type_name}")
    return None

  most_recent = results[0]
  s3_object_id = most_recent.s3_object_id
  if len(results) > 1:
    duplicate_object_ids = [x.s3_object_id for x in results]
    sys.stderr.write(f"Multiple results detected for {type_name} ({duplicate_object_ids}), taking most recent '{s3_object_id}'")

  s3_object = S3Object(
                key = most_recent.key,
                bucket_name = most_recent.bucket_name,
                s3_type_id = most_recent.s3_type_id,
              )

  return s3_object_id, s3_object


def getS3TypeIdByName(cursor : Any, type_name : str) -> TableId:
  stmt = """select s3_type_id from s3_type where type_name = %(type_name)s"""
  cursor.execute(stmt, {"type_name": type_name})
  results = cursor.fetchall()
  if len(results) <= 0:
    raise Exception(f"No type exists '%(type_name)s'",type_name)
  return TableId(results[0].s3_type_id)


