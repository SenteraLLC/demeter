from typing import Iterator

from ..lib.core import types as demeter_types

from ..importing.find_fn import FindFn
from ..importing.write_fn import WriteFn
from ..importing.get_fn import GetFn

from .util import getGeomGrowerField
from .util import MigrateArgs

from .imports import *

def makeGeomAndField(field_it : Iterator[GrowerField],
                     find_fn  : FindFn,
                     write_fn : WriteFn,
                     get_fn   : GetFn,
                     args     : MigrateArgs,
                    ):
  grower_field = next(field_it)

  ggf = getGeomGrowerField(find_fn, get_fn, args["owner_id"], grower_field)
  geom, grower, deferred_field = ggf

  write_fn(demeter_types.Geom, geom)
  write_fn(demeter_types.Field, deferred_field)
  write_fn(demeter_types.Grower, grower)


