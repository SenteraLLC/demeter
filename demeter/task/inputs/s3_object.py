from typing import Any, List, Optional, Tuple, Union
import sys

from ...db import TableId
from ...db.type_to_sql import generateInsertMany
from .types import S3ObjectKey, S3Object

from psycopg2 import sql
from ...db.type_to_sql import PGJoin, PGFormat

from ...data.core.types import Key


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


