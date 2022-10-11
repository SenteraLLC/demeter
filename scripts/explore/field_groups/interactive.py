from typing import Any, Sequence, List, Optional

import curses

from demeter.db import Table, TableId
from demeter.db import getConnection
from demeter.data import FieldGroup

from ..formatting import Margins

from ..picker import Picker

from . import FieldGroupSummary
from . import getFieldGroupSummaries

from ..curses import setup_curses

from .layout import LAYOUT

HEADER_ROW_COUNT = 5
FOOTER_ROW_COUNT = 4
FRAME_ROW_COUNT = HEADER_ROW_COUNT + FOOTER_ROW_COUNT

@setup_curses
def select(cursor : Any) -> Sequence[FieldGroupSummary]:

  margins = Margins(
    top = HEADER_ROW_COUNT,
    left = 1,
    right = 1,
    bottom = FOOTER_ROW_COUNT,
  )

  field_group_summaries = getFieldGroupSummaries(cursor)

  separation_width = 3
  separator= " " * separation_width

  picker = Picker('Field Group Summary', field_group_summaries, margins, LAYOUT)

  while (picker.step()):
    pass

  selected_rows = picker.get_selected_rows()
  return selected_rows


