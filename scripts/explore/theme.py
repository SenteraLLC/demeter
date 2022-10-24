import curses

from enum import IntEnum


class ColorScheme(IntEnum):
  DEFAULT = 123

  SELECTED = 1
  CURSOR = 2
  SELECTED_CURSOR = 3
  VALUE = 4
  MENU_TEXT = 5
  MENU_VALUE = 6
  MENU_ALERT = 7
  MENU_OTHER = 8

# Make some custom colors
COLOR_BRIGHT_WHITE = 66
COLOR_ORANGE = 77
COLOR_DARK_GRAY = 88
COLOR_LIGHT_GRAY = 99

MENU_PRIMARY = COLOR_DARK_GRAY
MENU_SECONDARY = COLOR_LIGHT_GRAY
BG = curses.COLOR_BLACK


def _scale(n : int) -> int:
  if n < 0 or n > 255:
    raise Exception(f"Bad value for scaling: {n}")
  return int((n / 256) * 1000) + 1


def _init(color_id : int, r : int, g : int, b : int) -> None:
  s = _scale
  curses.init_color(color_id, s(r), s(g), s(b))
  return None


def setup_theme() -> None:
  DG = COLOR_DARK_GRAY
  LG = COLOR_LIGHT_GRAY
  _init(DG, 40, 40, 40)
  _init(LG, 70, 70, 70)

  R = curses.COLOR_RED
  Y = curses.COLOR_YELLOW
  B = curses.COLOR_BLUE
  _init(R, 255, 111, 111)
  _init(Y, 255, 247, 111)
  _init(B, 141, 193, 247)

  O = COLOR_ORANGE
  QQQ = COLOR_ORANGE
  M = curses.COLOR_MAGENTA
  _init(M, 187, 87, 197)
  _init(O, 255, 199, 111)
  #_init(O, 255, 0, 0)
  # Skip green hue for accessibility

  K = curses.COLOR_BLACK
  W = curses.COLOR_WHITE
  XW = COLOR_BRIGHT_WHITE
  _init(K, 1, 1, 1)
  _init(W, 240, 240, 240)
  _init(XW, 255, 255, 255)

  curses.init_pair(ColorScheme.DEFAULT, XW, BG)
  curses.init_pair(ColorScheme.SELECTED, R, BG)
  curses.init_pair(ColorScheme.CURSOR, Y, MENU_SECONDARY)
  curses.init_pair(ColorScheme.SELECTED_CURSOR, O, MENU_SECONDARY)
  curses.init_pair(ColorScheme.VALUE, B, BG)

  curses.init_pair(ColorScheme.MENU_TEXT, B, MENU_PRIMARY)
  #curses.init_pair(ColorScheme.MENU_VALUE, W, MENU_SECONDARY)
  #curses.init_pair(ColorScheme.MENU_ALERT, R, MENU_SECONDARY)
  #curses.init_pair(ColorScheme.MENU_OTHER, W, MENU_SECONDARY)

