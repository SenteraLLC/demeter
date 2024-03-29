import curses
import math
from dataclasses import asdict, dataclass
from typing import (
    Any,
    Dict,
    Generic,
    List,
    NamedTuple,
    Optional,
    Sequence,
    Set,
)

from ..formatting import Margins, Specifiers
from ..log import getLogger
from ..summary import RawRowType
from ..theme import ColorScheme

logger = getLogger()


@dataclass
class Row(Generic[RawRowType]):
    raw: RawRowType
    formatted: str


class Coords(NamedTuple):
    top: int
    left: int
    bottom: int
    right: int


class Table(Generic[RawRowType]):
    def __init__(
        self,
        raw_rows: Sequence[RawRowType],
        specifiers: Specifiers,
        margins: Margins,
        y_max: int,
        x_max: int,
        separator: str,
    ) -> None:
        self.raw_rows = raw_rows
        self.specifiers = specifiers
        self.margins = margins
        self.y_max = y_max
        self.x_max = x_max
        self.separator = separator

        self.top = self.margins.top
        self.bottom = self.y_max - self.margins.bottom - 1
        self.left = self.margins.left
        self.right = self.x_max - self.margins.right - 1

        top, left, bottom, right = self.get_coords()
        self.height = h = bottom - top
        self.width = right - left

        self.cursor_offset = 0
        self.cursor_buffer = math.floor(math.sqrt(h))
        self.page_offset = 0

        self.number_of_rows = len(raw_rows)
        n = self.number_of_rows
        self.max_page_offset = 0 if h >= n else n - h
        self.number_of_visible_rows = min(h, n)

        self.mypad = curses.newpad(n + 1, self.width)

        rows: List[Row[RawRowType]] = []
        for i, r in enumerate(self.raw_rows):
            f = self.format_row(asdict(r), self.width)
            rows.append(Row(r, f))

        self.rows: Sequence[Row[RawRowType]] = rows

    def draw(self, selected: Set[int]) -> bool:
        for i, r in enumerate(self.rows):
            f = r.formatted
            color_scheme = ColorScheme.DEFAULT
            if i in selected:
                color_scheme = ColorScheme.SELECTED
            self.mypad.addnstr(i, 0, f, self.width, curses.color_pair(color_scheme))
        return True

    def get_coords(self) -> Coords:
        return Coords(self.top, self.left, self.bottom, self.right)

    def format_row(
        self,
        row: Dict[str, Any],
        maybe_width: Optional[int] = None,
    ) -> str:
        # parts : List[str] = ["  "]
        parts: List[str] = []
        for key, specifier in self.specifiers.values():
            # f = self.layout[t]
            v = row[key]
            contents = str(v) if v is not None else ""
            p = f"{contents:{specifier}}"
            parts.append(p)

        # logger.warning("PARTS: %s", parts)
        row_as_string = self.separator.join(parts)
        s = row_as_string
        w = maybe_width
        return s if (w is None) else s.center(w)

    # def format_header(self) -> str:
    #  raw_header = { n : n for n in asdict(self.raw_rows[0]) }
    #  return self.format_row(raw_header, self.width)

    def refresh_row(
        self,
        cursor_offset: int,
        selected_rows: Set[int],
        color_scheme: ColorScheme = ColorScheme.DEFAULT,
        selected_color_scheme: Optional[ColorScheme] = ColorScheme.SELECTED,
    ) -> bool:
        co = cursor_offset
        if co in selected_rows:
            if (s := selected_color_scheme) is not None:
                color_scheme = s
        f = self.rows[co].formatted
        self.mypad.addnstr(co, 0, f, self.width, curses.color_pair(color_scheme))
        return True

    def refresh_cursor(
        self,
        cursor_offset: int,
        selected_rows: Set[int],
    ) -> bool:
        return self.refresh_row(
            cursor_offset,
            selected_rows,
            ColorScheme.CURSOR,
            ColorScheme.SELECTED_CURSOR,
        )

    def refresh(self, selected_rows: Set[int]) -> None:
        self.mypad.move(self.cursor_offset, 1)
        self.refresh_cursor(self.cursor_offset, selected_rows)
        # logger.warning("OUT: %s %s %s %s",self.top, self.left, self.bottom, self.right)
        self.mypad.refresh(
            self.page_offset, 0, self.top, self.left, self.bottom, self.right
        )

    def update_cursor(self, dy: int, selected_rows: Set[int]) -> bool:
        co = self.cursor_offset
        po = self.page_offset
        if ((maybe_cursor_offset := co + dy) < self.number_of_rows) and (
            maybe_cursor_offset >= 0
        ):
            if self.max_page_offset > 0:
                dfs = co - po
                dfe = self.height - dfs

                cb = self.cursor_buffer
                is_in_cursor_buffer = (dfs < cb) if dy < 0 else (dfe < cb)
                if is_in_cursor_buffer:
                    mpo = po + dy
                    if mpo >= 0 and mpo <= self.max_page_offset:
                        self.page_offset = mpo

            self.refresh_row(co, selected_rows)

            co = self.cursor_offset = maybe_cursor_offset
            self.mypad.move(co, 1)

            self.refresh_cursor(co, selected_rows)

            return True
        return False

    def get_curses_command(self) -> int:
        return self.mypad.getch()

    def get_page_offset(self) -> int:
        return self.page_offset

    def get_cursor_offset(self) -> int:
        return self.cursor_offset

    def get_width(self) -> int:
        return self.width

    def get_rows(self) -> Sequence[Row[RawRowType]]:
        return self.rows

    def get_number_of_rows(self) -> int:
        return self.number_of_rows

    def get_number_of_visible_rows(self) -> int:
        return self.number_of_visible_rows
