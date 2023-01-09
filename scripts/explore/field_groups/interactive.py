from collections import OrderedDict
from typing import Any

from ..curses import (
    FilterBy,
    SelectedResult,
    setup_curses,
)
from ..formatting import Margins
from ..log import getLogger
from ..picker.tree import PickerTree
from . import FieldGroupSummary, getFieldGroupSummaries
from .layout import LAYOUT

logger = getLogger()

HEADER_ROW_COUNT = 5
FOOTER_ROW_COUNT = 4
FRAME_ROW_COUNT = HEADER_ROW_COUNT + FOOTER_ROW_COUNT


@setup_curses
def select(
    cursor: Any, filter_by: FilterBy = OrderedDict()
) -> SelectedResult[FieldGroupSummary]:
    margins = Margins(
        top=HEADER_ROW_COUNT,
        left=1,
        right=1,
        bottom=FOOTER_ROW_COUNT,
    )

    parcel_group_summaries = getFieldGroupSummaries(cursor)

    # TODO: Add dynamic title for tree path
    # TODO: Add dynamic footer showing selected value metadata

    # Display as 'Field Groups'
    picker = PickerTree("Field Groups", margins, LAYOUT, parcel_group_summaries)

    while picker.step():
        pass

    selected_rows = picker.get_selected_rows()
    return SelectedResult(selected=selected_rows, results=parcel_group_summaries)
