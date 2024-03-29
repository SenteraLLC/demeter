from collections import OrderedDict
from typing import Any

from ..curses import (
    FilterBy,
    SelectedResult,
    setup_curses,
)
from ..fields import getFieldIds
from ..formatting import Margins
from ..log import getLogger
from ..picker import Picker
from . import TypeSummary, getTypeSummaries
from .layout import LAYOUT

# from demeter.data import getFieldGroupFields


HEADER_ROW_COUNT = 5
FOOTER_ROW_COUNT = 4
FRAME_ROW_COUNT = HEADER_ROW_COUNT + FOOTER_ROW_COUNT


logger = getLogger()


@setup_curses
def select(
    cursor: Any, filter_by: FilterBy = OrderedDict()
) -> SelectedResult[TypeSummary]:
    logger.warning("START SELECT.")

    margins = Margins(
        top=HEADER_ROW_COUNT,
        left=1,
        right=1,
        bottom=FOOTER_ROW_COUNT,
    )

    logger.warning("START SELECT.")

    field_ids = getFieldIds(filter_by)
    logger.warning("Field IDs are: %s\n", field_ids)

    observation_type_summaries = getTypeSummaries(cursor, list(field_ids))

    id_to_summaries = OrderedDict(
        (t.observation_type_id, t) for t in observation_type_summaries
    )

    # separation_width = 3
    # separator = " " * separation_width

    picker = Picker(
        "Observation Type Summary", observation_type_summaries, margins, LAYOUT
    )

    while picker.step():
        logger.warning("Type picker step.")
        pass

    selected_rows = picker.get_selected_rows()
    return SelectedResult(
        selected=selected_rows,
        results=id_to_summaries,
    )
