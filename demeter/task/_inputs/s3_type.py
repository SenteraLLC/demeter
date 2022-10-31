from typing import Any, Callable, Mapping, Optional, Tuple, Type

from ...db import SomeKey, TableId
from .generated import (getMaybeS3TypeDataFrame, getS3TypeBase,
                        insertOrGetS3Type, insertS3TypeDataFrame)
from .types import S3SubType, S3Type, S3TypeDataFrame, TaggedS3SubType


def insertOrGetS3TypeDataFrame(
    cursor: Any,
    s3_type: S3Type,
    s3_type_dataframe: S3TypeDataFrame,
) -> TableId:
    s3_type_id = insertOrGetS3Type(cursor, s3_type)
    stmt = """insert into s3_type_dataframe(s3_type_id, driver, has_geometry)
            values(%(s3_type_id)s, %(driver)s, %(has_geometry)s)
            on conflict do nothing"""
    args = {
        "s3_type_id": s3_type_id,
        "driver": s3_type_dataframe.driver,
        "has_geometry": s3_type_dataframe.has_geometry,
    }
    cursor.execute(stmt, args)

    return s3_type_id


s3_sub_type_get_lookup: Mapping[
    Type[S3SubType], Callable[[Any, TableId], S3SubType]
] = {S3TypeDataFrame: getMaybeS3TypeDataFrame}

s3_sub_type_insert_lookup: Mapping[Type[S3SubType], Callable[[Any, Any], SomeKey]] = {
    S3TypeDataFrame: insertS3TypeDataFrame
}


def insertS3Type(
    cursor: Any,
    s3_type: S3Type,
    s3_sub_type: Optional[S3SubType],
) -> TableId:
    s3_type_id = insertOrGetS3Type(cursor, s3_type)
    if s3_sub_type is not None:
        sub_type_insert_fn = s3_sub_type_insert_lookup[type(s3_sub_type)]
        s3_sub_type_key = sub_type_insert_fn(cursor, s3_sub_type)
    return s3_type_id


def getS3Type(
    cursor: Any,
    s3_type_id: TableId,
) -> Tuple[S3Type, Optional[TaggedS3SubType]]:
    s3_type = getS3TypeBase(cursor, s3_type_id)
    maybe_s3_sub_type = None
    for s3_sub_type_tag, s3_sub_type_get_lookup_fn in s3_sub_type_get_lookup.items():
        maybe_s3_sub_type = s3_sub_type_get_lookup_fn(cursor, s3_type_id)
        if maybe_s3_sub_type is not None:
            s3_sub_type = maybe_s3_sub_type
            s3_subtype_value = TaggedS3SubType(
                tag=s3_sub_type_tag,
                value=s3_sub_type,
            )
            return s3_type, s3_subtype_value

    return s3_type, None


def getS3TypeIdByName(cursor: Any, type_name: str) -> TableId:
    stmt = """select s3_type_id from s3_type where type_name = %(type_name)s"""
    cursor.execute(stmt, {"type_name": type_name})
    results = cursor.fetchall()
    if len(results) <= 0:
        raise Exception(f"No type exists '%(type_name)s'", type_name)
    return TableId(results[0].s3_type_id)
