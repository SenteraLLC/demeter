import curses
from dataclasses import asdict
from typing import Sequence

from ..formatting import Margins
from ..summary import RawRowType
from ..theme import ColorScheme
from .table import Row


class Footer:
    def __init__(
        self,
        width: int,
        margins: Margins,
        table_bottom: int,
    ) -> None:
        self.width = width
        self.margins = margins
        self.footer = curses.newwin(
            self.margins.bottom + 1, width, table_bottom + 1, margins.left
        )

    def refresh(
        self,
        row: RawRowType,
    ) -> None:
        if (height := self.margins.bottom) > 0:
            # TODO: Improve prettification
            # NOTE: 's' is consumed by the for-loop
            s = str(asdict(row))

            w = self.width
            for i in range(height):
                next_line, s = s[:w], s[w:]
                next_line = f"{next_line:<{w}.{w}}"
                self.footer.addnstr(
                    i, 1, next_line, w - 1, curses.color_pair(ColorScheme.MENU_TEXT)
                )

        self.footer.refresh()
