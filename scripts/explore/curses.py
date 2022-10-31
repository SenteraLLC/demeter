import curses
from collections import OrderedDict
from dataclasses import dataclass
from typing import Any, Callable, Generic, NewType, Sequence, TypeVar

from demeter.db import TableId

from .data_option import DataOption
from .summary import Summary

S = TypeVar("S")


@dataclass
class SelectedResult(Generic[S]):
    selected: Sequence[S]
    results: OrderedDict[TableId, S]


FilterBy = OrderedDict[DataOption, SelectedResult[Summary]]


SelectionFunction = Callable[[Any, FilterBy], SelectedResult[S]]


def setup_curses(fn: SelectionFunction[S]) -> SelectionFunction[S]:
    def inner(cursor: Any, filter_by: FilterBy) -> SelectedResult[S]:
        stdscr = curses.initscr()
        curses.start_color()
        curses.noecho()
        curses.cbreak()
        curses.curs_set(0)

        stdscr.keypad(True)

        result = fn(cursor, filter_by)

        curses.curs_set(1)

        curses.nocbreak()
        stdscr.keypad(False)
        curses.echo()
        curses.endwin()
        return result

    return inner
