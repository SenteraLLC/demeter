import json
import os
import uuid
from typing import Any, Tuple, Union

import geopandas as gpd  # type: ignore
import pandas as pd
import psycopg2
import psycopg2.extras
from scoping import scoping  # type: ignore

from demeter.db import TableId
from demeter.db import getConnection as getPgConnection
from demeter.task import (
    S3Type,
    S3TypeDataFrame,
    TaggedS3SubType,
    getS3Connection,
    insertOrGetS3TypeDataFrame,
)
from demeter.work import DataSource, S3File
from demeter.work._util.cli import parseCLIArguments
from demeter.work._util.keys import loadKeys

VERSION = 1

if __name__ == "__main__":
    connection = getPgConnection()
    cursor = connection.cursor()

    (s3_connection, bucket_name) = getS3Connection()

    kwargs, default_cli_kwargs = parseCLIArguments(__file__, VERSION)
    geospatial_key_file = default_cli_kwargs["geospatial_key_file"]
    temporal_key_file = default_cli_kwargs["temporal_key_file"]
    test_keys = list(loadKeys(cursor, geospatial_key_file, temporal_key_file))

    datasource = DataSource(
        test_keys, TableId(0), TableId(0), cursor, s3_connection, {}, {}
    )

    def newS3TypeDataFrame(
        type_name: str, driver: str, has_geometry: bool
    ) -> Tuple[TableId, TaggedS3SubType]:
        s3_type = S3Type(
            type_name=type_name,
        )
        s3_type_dataframe = S3TypeDataFrame(
            driver=driver,
            has_geometry=has_geometry,
        )
        s3_type_id = insertOrGetS3TypeDataFrame(cursor, s3_type, s3_type_dataframe)

        tagged_s3_subtype = TaggedS3SubType(
            tag=S3TypeDataFrame,
            value=s3_type_dataframe,
        )

        return s3_type_id, tagged_s3_subtype

    newS3TypeDataFrame("test_geojson_type", "GeoJSON", True)

    geoms = datasource.getGeometry()

    ns = pd.Series(range(0, len(geoms)))

    geoms_df = geoms.assign(N=ns)

    test_file_prefix = "s3_test_script"

    def uploadTestFile(
        value: Union[pd.DataFrame, gpd.GeoDataFrame],
        type_name: str,
        driver: str,
        has_geometry: bool,
    ) -> None:
        s3_type_id, s3_type_dataframe = newS3TypeDataFrame(
            type_name, driver, has_geometry
        )

        to_upload = S3File(value, test_file_prefix)
        s3_file_meta = to_upload.to_file(s3_type_dataframe)
        datasource.upload_file(s3_type_id, bucket_name, s3_file_meta)

    # Geometric type
    with scoping():
        type_name = "my_test_geo_type"
        driver = "GeoJSON"
        has_geometry = True
        uploadTestFile(geoms_df, type_name, driver, has_geometry)

    geom_id_column = geoms_df["geom_id"]
    nongeoms_df = pd.DataFrame().assign(geom_id=geom_id_column, N=ns)

    # Non-Geometric type
    with scoping():
        type_name = "my_test_nongeo_type"
        driver = "json"
        has_geometry = False
        uploadTestFile(nongeoms_df, type_name, driver, has_geometry)

    connection.commit()
