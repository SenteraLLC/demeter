import curses
from typing import Generic, List, cast

from ..formatting import Margins
from ..theme import ColorScheme


class Header:
    def __init__(
        self,
        title: str,
        header: str,
        width: int,
        margins: Margins,
    ) -> None:
        self.title = title
        self.width = width
        self.margins = margins

        h = self.margins.top
        self.header = curses.newwin(h + 1, width, 0, margins.left)
        w = width
        self.header.addnstr(3, 0, header, w, curses.color_pair(ColorScheme.MENU_TEXT))

    def refresh(
        self, page_offset: int, number_of_rows: int, number_of_visible_rows: int
    ) -> None:
        start = page_offset
        l = f" {self.title}"
        end = start + number_of_visible_rows - 1
        r = f"Showing [{start} - {end}] of {number_of_rows} "
        buffer = " " * (self.width - len(l) - len(r))
        msg = "".join([l, buffer, r])
        self.header.addnstr(
            1, 0, msg, self.width, curses.color_pair(ColorScheme.MENU_TEXT)
        )
        self.header.refresh()
