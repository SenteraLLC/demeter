import curses
import curses.ascii
from collections import OrderedDict
from enum import IntEnum
from typing import (
    Generic,
    Optional,
    Sequence,
    Set,
)

from ..formatting import (
    ColumnFormat,
    Margins,
    getSpecifiers,
    inferColumnFormat,
)
from ..log import getLogger
from ..summary import RawRowType
from ..theme import ColorScheme, setup_theme
from .footer import Footer
from .header import Header
from .table import Table

logger = getLogger()


def to_ascii(s: str) -> int:
    return ord(curses.ascii.ascii(s))


class Command(IntEnum):
    UP = to_ascii("k")
    DOWN = to_ascii("j")
    SELECT = to_ascii(" ")
    ENTER = to_ascii("\n")
    BACK = to_ascii("q")
    QUIT = 27


class Picker(Generic[RawRowType]):
    def __init__(
        self,
        title: str,
        raw_rows: Sequence[RawRowType],
        margins: Margins,
        layout: OrderedDict[str, ColumnFormat],
        separator: str = "   ",
    ) -> None:
        setup_theme()

        self.layout = layout
        for t, f in self.layout.items():
            inferColumnFormat(f, t, raw_rows)

        self.separator = separator
        self.specifiers = getSpecifiers(layout, self.separator, curses.COLS)

        self.title = title
        self.raw_rows = raw_rows
        self.margins = margins
        self.selected_rows: Set[int] = set()

        self.y_max = curses.LINES
        self.x_max = curses.COLS
        self.number_of_rows = len(self.raw_rows)

        self.table = Table(
            self.raw_rows,
            self.specifiers,
            self.margins,
            self.y_max,
            self.x_max,
            separator,
        )

        w = self.table.get_width()
        m = self.margins

        h = self.table.format_row({layout[t].key: t for t in layout}, w)
        # h = self.table.format_header()
        self.header = Header(title, h, w, m)

        _ = self.table.get_rows()
        coords = self.table.get_coords()
        self.footer = Footer(w, m, coords.bottom)

        self.table.draw(self.selected_rows)
        self.refresh()

    def refresh(self) -> None:
        po = self.table.get_page_offset()
        n = self.table.get_number_of_rows()
        v = self.table.get_number_of_visible_rows()
        self.header.refresh(po, n, v)

        _ = self.table.get_cursor_offset()
        row = self.get_cursor_row()
        self.footer.refresh(row)

        self.table.refresh(self.selected_rows)

    def get_command(self) -> int:
        cmd = self.table.get_curses_command()
        logger.warning("CMD: %s", str(cmd))
        return cmd

    def step(self, maybe_command: Optional[int] = None) -> bool:
        if maybe_command is None:
            maybe_command = self.get_command()
        cmd = maybe_command

        keep_going = True
        if cmd == Command.QUIT:
            return self._do_quit()
        elif cmd == Command.SELECT:
            keep_going = self._do_select()
        elif cmd == Command.ENTER:
            keep_going = self._do_enter()
        else:
            dy = {
                Command.UP: -1,
                curses.KEY_UP: -1,
                Command.DOWN: 1,
                curses.KEY_DOWN: 1,
            }.get(cmd, 0)
            if dy != 0:
                keep_going = self._do_navigate(dy)

        self.refresh()

        return keep_going

    def _do_select(self) -> bool:
        co = self.table.get_cursor_offset()
        _ = ColorScheme.DEFAULT
        try:
            self.selected_rows.remove(co)
            self.table.refresh_row(co, self.selected_rows, ColorScheme.CURSOR)
        except KeyError:
            self.selected_rows.add(co)
            self.table.refresh_cursor(co, self.selected_rows)
        return True

    def _do_navigate(self, dy: int) -> bool:
        if dy != 0:
            _ = self.table.update_cursor(dy, self.selected_rows)
            return True
        return False

    def _do_enter(self) -> bool:
        return True

    def _do_back(self) -> bool:
        return True

    def _do_quit(self) -> bool:
        return False

    def get_selected_rows(self) -> Sequence[RawRowType]:
        return [self.table.raw_rows[i] for i in self.selected_rows]

    def get_cursor_row(self) -> RawRowType:
        co = self.table.get_cursor_offset()
        return self.table.raw_rows[co]

    def is_selected(self) -> bool:
        co = self.table.get_cursor_offset()
        return co in self.selected_rows
