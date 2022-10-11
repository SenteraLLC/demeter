from typing import Callable, Sequence, TypeVar, Any, NewType

import curses

S = TypeVar('S')
SelectionFunction = Callable[[Any], Sequence[S]]

def setup_curses(fn : SelectionFunction[S]) -> SelectionFunction[S]:
  def inner(cursor : Any) -> Sequence[S]:
    stdscr = curses.initscr()
    curses.start_color()
    curses.noecho()
    curses.cbreak()
    curses.curs_set(0)

    stdscr.keypad(True)

    result = fn(cursor)

    curses.curs_set(1)

    curses.nocbreak()
    stdscr.keypad(False)
    curses.echo()
    curses.endwin()
    return result
  return inner


