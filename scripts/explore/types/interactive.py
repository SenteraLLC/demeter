import curses
from collections import OrderedDict
from typing import Any, Dict, List, Optional, Sequence, Set, cast

from demeter.data import LocalType, LocalValue
from demeter.db import Table, TableId, getConnection

from ..curses import FilterBy, SelectedResult, setup_curses
from ..data_option import DataOption
from ..field_groups import FieldGroupSummary
from ..fields import getFieldIds
from ..formatting import Margins
from ..picker import Picker
from . import TypeSummary, getTypeSummaries
from .layout import LAYOUT

# from demeter.data import getFieldGroupFields


HEADER_ROW_COUNT = 5
FOOTER_ROW_COUNT = 4
FRAME_ROW_COUNT = HEADER_ROW_COUNT + FOOTER_ROW_COUNT

from ..log import getLogger

logger = getLogger()


@setup_curses
def select(
    cursor: Any, filter_by: FilterBy = OrderedDict()
) -> SelectedResult[TypeSummary]:
    logger.warn("START SELECT.")

    margins = Margins(
        top=HEADER_ROW_COUNT,
        left=1,
        right=1,
        bottom=FOOTER_ROW_COUNT,
    )

    logger.warn("START SELECT.")

    field_ids = getFieldIds(filter_by)
    logger.warn("Field IDs are: %s\n", field_ids)

    local_type_summaries = getTypeSummaries(cursor, list(field_ids))

    id_to_summaries = OrderedDict((t.local_type_id, t) for t in local_type_summaries)

    separation_width = 3
    separator = " " * separation_width

    picker = Picker("Local Type Summary", local_type_summaries, margins, LAYOUT)

    while picker.step():
        logger.warn("Type picker step.")
        pass

    selected_rows = picker.get_selected_rows()
    return SelectedResult(
        selected=selected_rows,
        results=id_to_summaries,
    )
