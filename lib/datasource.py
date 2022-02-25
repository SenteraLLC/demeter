import itertools

from typing import List, Optional, TypedDict, Tuple, Generator, TypeVar, Generic, Any

import psycopg2

from datetime import date

from . import schema_api
from .connections import *
from .types import GeoSpatialKey, TemporalKey, LocalType, LocalValue, UnitType

from . import local

# TODO: Stubs?
from shapely import wkb  # type: ignore


class DataSource(object):
  def __init__(self,
               geospatial_keys : List[GeoSpatialKey],
               temporal_keys   : List[TemporalKey],
               pg_connection   : PGConnection,
               s3_connection   : S3Connection,
              ):
    self.geospatial_keys = geospatial_keys
    self.temporal_keys = temporal_keys
    self.pg_connection = pg_connection
    self.s3_connection = s3_connection
    self.local = DataSource.Local(self)


  # TODO: Can we always assume cartesian products?
  def keys(self) -> Generator[Tuple[GeoSpatialKey, TemporalKey], None, None]:
    geospatial = (g for g in self.geospatial_keys)
    temporal = (t for t in self.temporal_keys)

    for g, t in itertools.product(geospatial, temporal):
      cursor = self.pg_connection.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

      yield g, t

  def key(self) -> Any:
    return {}

  # Namespace
  class Local(object):
    def __init__(self, datasource : 'DataSource'):
      self.datasource = datasource

    def load(self, local_type : LocalType) -> List[Tuple[LocalValue, UnitType]]:
      cursor = self.datasource.pg_connection.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

      results : List[Tuple[LocalValue, UnitType]] = []
      for g, t in self.datasource.keys():

        partial_results = local._load(cursor, g, t, local_type)
        results.extend(partial_results)
      return results




class OutputObject(object):
  pass

T = TypeVar('T')

class S3(Generic[T]):
  def __init__(self, v : T):
    self.value = v
