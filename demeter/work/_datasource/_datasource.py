import uuid
from collections import OrderedDict
from io import BytesIO
from typing import Any, Callable, Dict, List, Mapping, Optional, Set, Tuple, Type, cast

import geopandas as gpd  # type: ignore
import pandas as pd

from ... import data, db, task
from ._base import DataSourceBase
from ._http import getHTTPRows
from ._observation import getObservationRows
from ._response import KeyToArgsFunction, OneToOneResponseFunction, ResponseFunction
from ._s3 import getRawS3, rawToDataFrame
from ._s3_file import AnyDataFrame, S3FileMeta, SupportedS3DataType
from ._util import createKeywordArguments

JoinResults = Dict[frozenset[str], AnyDataFrame]


# TODO: Defer S3 downloads until joining or manually altered
class DataSource(DataSourceBase):
    def __init__(
        self,
        keys: List[data.Key],
        function_id: db.TableId,
        execution_id: db.TableId,
        cursor: Any,
        s3_connection: Any,
        keyword_arguments: Dict[str, Any],
        keyword_types: Dict[str, Type[Any]],
    ):
        super().__init__(cursor, function_id, execution_id)

        self.s3_connection = s3_connection
        self.cursor = cursor
        self.keys = self.execution_summary.inputs["keys"] = keys
        self.execution_summary.inputs["keyword"] = createKeywordArguments(
            keyword_arguments, keyword_types, execution_id, function_id
        )

        self.dataframes: Dict[str, pd.DataFrame] = OrderedDict()
        self.geodataframes: Dict[str, gpd.GeoDataFrame] = OrderedDict()
        self.explicit_joins: Dict[
            Tuple[str, str],
            Tuple[Optional[Callable[..., AnyDataFrame]], Dict[str, Any]],
        ] = {}

    def _observation(self, observation_types: List[data.ObservationType]) -> pd.DataFrame:
        if self.OBSERVATION in self.dataframes:
            raise Exception("Observation data can only be acquired once.")

        rows = getObservationRows(self.cursor, self.keys, observation_types, self.execution_summary)
        df = pd.DataFrame(rows)
        self.dataframes[self.OBSERVATION] = df
        return df

    # Does not support GeoDataFrames via http
    def _http(
        self,
        type_name: str,
        param_fn: Optional[KeyToArgsFunction] = None,
        json_fn: Optional[KeyToArgsFunction] = None,
        response_fn: ResponseFunction = OneToOneResponseFunction,
        http_options: Dict[str, Any] = {},
    ) -> pd.DataFrame:
        raw_results = getHTTPRows(
            self.cursor,
            self.keys,
            self.execution_summary,
            type_name,
            param_fn,
            json_fn,
            response_fn,
            http_options,
        )
        results: List[Dict[str, Any]] = []
        for key, row in raw_results:
            results.append(dict(**key(), **row))

        df = pd.DataFrame(results)
        self.dataframes[type_name] = df
        return df

    def _s3(
        self,
        type_name: str,
    ) -> SupportedS3DataType:
        raw_results, maybe_tagged_s3_subtype = getRawS3(
            self.cursor,
            self.s3_connection,
            self.keys,
            type_name,
            self.execution_summary,
        )
        if maybe_tagged_s3_subtype is not None:
            tagged_s3_subtype = maybe_tagged_s3_subtype
            tag = tagged_s3_subtype.tag
            subtype = tagged_s3_subtype.value
            if tag == task.S3TypeDataFrame:
                dataframe_subtype = task.S3TypeDataFrame(**subtype())
                maybe_df, maybe_gdf = rawToDataFrame(raw_results, dataframe_subtype)
                if maybe_df is not None:
                    df = self.dataframes[type_name] = maybe_df
                    return df
                elif maybe_gdf is not None:
                    gdf = self.geodataframes[type_name] = maybe_gdf
                    return gdf
                else:
                    raise Exception("Unknown dataframe.")

        return raw_results

    def upload(
        self,
        s3_type_id: db.TableId,
        bucket_name: str,
        blob: BytesIO,
        s3_key: str = str(uuid.uuid4()),
    ) -> db.TableId:
        self.s3_connection.Bucket(bucket_name).upload_fileobj(Key=s3_key, Fileobj=blob)
        s3_object = task.S3Object(
            s3_type_id=s3_type_id,
            key=s3_key,
            bucket_name=bucket_name,
        )
        s3_object_id = task.insertS3Object(self.cursor, s3_object)
        task.insertS3ObjectKeys(self.cursor, s3_object_id, self.keys, s3_type_id)
        return s3_object_id

    def upload_file(
        self,
        s3_type_id: db.TableId,
        bucket_name: str,
        s3_file_meta: S3FileMeta,
    ) -> db.TableId:
        s3_filename_on_disk = s3_file_meta["filename_on_disk"]
        f = open(s3_filename_on_disk, "rb")
        as_bytes = BytesIO(f.read())

        s3_key = s3_file_meta["key"]
        return self.upload(s3_type_id, bucket_name, as_bytes, s3_key)

    def getGeometry(self) -> gpd.GeoDataFrame:
        geo_ids = [k.geom_id for k in self.keys]
        stmt = """
             select G.geom_id, G.geom, G.container_geom_id
             from geom G
             join geom CONTAINER
               on G.geom_id = CONTAINER.geom_id
             where G.geom_id = any(%(geo_ids)s)
           """
        result = gpd.read_postgis(
            stmt, self.cursor.connection, "geom", params={"geo_ids": geo_ids}
        )
        return result

    def join(
        self,
        left_type_name: str,
        right_type_name: str,
        join_fn: Optional[Callable[..., Any]] = None,
        **kwargs: Any,
    ) -> None:
        dataframe_names = set(self.dataframes.keys()).union(self.geodataframes.keys())
        dataframe_names.add(self.OBSERVATION)
        dataframe_names.add(self.GEOM)

        if left_type_name not in dataframe_names:
            raise Exception(f"No typename {left_type_name} found.")
        if right_type_name not in dataframe_names:
            raise Exception(f"No typename {right_type_name} found.")

        self.explicit_joins[(left_type_name, right_type_name)] = (join_fn, kwargs)

    def popDataFrame(self, type_name: str) -> Optional[AnyDataFrame]:
        if type_name == self.GEOM:
            return self.getGeometry()
        return (
            geo
            if (geo := self.geodataframes.pop(type_name, None)) is not None
            else self.dataframes.pop(type_name)
        )

    def _findExisting(
        self,
        type_name: str,
        explicit_join_results: JoinResults,
    ) -> Optional[frozenset[str]]:
        for type_names, existing_result in explicit_join_results.items():
            if type_name in type_names:
                return frozenset(type_names)
        return None

    def getDataFrame(
        self, type_name: str, explicit_join_results: JoinResults
    ) -> AnyDataFrame:
        maybe_df = self.popDataFrame(type_name)
        if (df := maybe_df) is not None:
            return df
        for types, df in explicit_join_results.items():
            if type_name in types:
                return df
        if type_name == self.GEOM:
            return self.getGeometry()
        raise Exception(f"Can't find data type {type_name}.")

    def doExplicitJoins(self) -> JoinResults:
        explicit_join_results: JoinResults = {}

        def addExplicitJoin(
            left_type_name: str,
            right_type_name: str,
            result: AnyDataFrame,
        ) -> bool:
            left_existing = self._findExisting(left_type_name, explicit_join_results)
            right_existing = self._findExisting(right_type_name, explicit_join_results)

            k = {left_type_name, right_type_name}
            if right_existing is not None:
                del explicit_join_results[right_existing]
                k.update(right_existing)
            if left_existing is not None:
                del explicit_join_results[left_existing]
                k.update(left_existing)
            explicit_join_results[frozenset(k)] = result
            return True

        for (left_type_name, right_type_name), (
            join_fn,
            kwargs,
        ) in self.explicit_joins.items():
            left = self.getDataFrame(left_type_name, explicit_join_results)
            right = self.getDataFrame(right_type_name, explicit_join_results)

            if join_fn is None:
                if isinstance(left, gpd.GeoDataFrame) and isinstance(
                    right, gpd.GeoDataFrame
                ):
                    join_fn = gpd.GeoDataFrame.sjoin
                else:
                    join_fn = pd.DataFrame.merge
            result = join_fn(left, right, **kwargs)
            addExplicitJoin(left_type_name, right_type_name, result)

        # TODO: Need to do something smarter,
        #       This should take into consideration the placement of the join
        #       in the script
        for types, compound_df in explicit_join_results.items():
            k = "_".join(types)
            if isinstance(compound_df, gpd.GeoDataFrame):
                self.geodataframes[k] = compound_df
            elif isinstance(compound_df, pd.DataFrame):
                self.dataframes[k] = compound_df

        return explicit_join_results

    def getMatrix(self) -> gpd.GeoDataFrame:
        all_dataframe_names = set(self.dataframes.keys()).union(
            self.geodataframes.keys()
        )
        joined_dataframe_names: Set[str] = set()

        explicit_join_results = self.doExplicitJoins()

        out: gpd.GeoDataFrame = self.getGeometry()

        for k, df in self.dataframes.items():
            out = out.merge(df, on="geom_id", how="inner")

        for k, gdf in self.geodataframes.items():
            left_suffix = str(uuid.uuid4())
            out = out.sjoin(gdf, lsuffix=left_suffix, rsuffix=k)

            ljoin: Callable[[str], str] = lambda s: "_".join([s, left_suffix])
            to_rename = {"geom_id", "container_geom_id"}
            out.rename(columns={ljoin(s): s for s in to_rename}, inplace=True)

        return out
