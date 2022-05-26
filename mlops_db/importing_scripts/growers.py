from typing import Iterator, Sequence

from ..importing.find_fn import FindFn
from ..importing.write_fn import WriteFn
from ..importing.get_fn import GetFn

from ..lib.core import types as demeter_types

from .util import makeGrower
from . import imports



def makeGrowers(grower_iter : Iterator[imports.Grower],
                find_fn     : FindFn,
                write_fn    : WriteFn,
                get_fn      : GetFn,
                owner_id    : int,
               ):
  input_grower = next(grower_iter)
  g = makeGrower(input_grower, find_fn, owner_id)
  gs : Sequence[demeter_types.Grower] = [] if g is None else [g]
  write_fn(demeter_types.Grower, gs)


