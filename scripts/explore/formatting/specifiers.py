from typing import Set, NamedTuple
from collections import OrderedDict

from .column import ColumnFormat


ColumnTitleToFormat = OrderedDict[str, ColumnFormat]


class KeyAndSpecifier(NamedTuple):
    key: str
    specifier: str


Specifiers = OrderedDict[str, KeyAndSpecifier]


def get_candidate_titles(
        selected_titles: Set[str],
        title_to_format: ColumnTitleToFormat,
        is_first_pass: bool,
    ) -> Set[str]:
    candidate_titles: Set[str] = set()
    for t, f in title_to_format.items():
        is_not_selected = t not in selected_titles

        do_not_skip = not f.shouldSkip(selected_titles)
        is_required = f.isRequired()
        do_optional = not is_first_pass
        is_candidate = do_not_skip and (is_required or do_optional)

        if is_not_selected and is_candidate:
            candidate_titles.add(t)
    return candidate_titles


def maybe_select_title(
    candidate_titles: Set[str],
    selected_titles: Set[str],
    title_to_format: ColumnTitleToFormat,
    remaining_width: int,
    separation_width: int,
) -> None:
    for t in candidate_titles:
        f = title_to_format[t]
        w = f.getWidth(t)
        maybe_remaining_width = remaining_width - w - separation_width
        if maybe_remaining_width >= 0:
            remaining_width = maybe_remaining_width
            selected_titles.add(t)


def getSpecifiers(
    title_to_format: ColumnTitleToFormat,
    separator: str,
    total_width: int,
) -> Specifiers:
    separation_width = len(separator)
    quit_threshold = int(abs(separation_width * 2) + 2)

    starting_width = total_width + 1
    remaining_width = starting_width - 1

    is_first_pass = True
    selected_titles: Set[str] = set()

    while (remaining_width > quit_threshold) and (starting_width > remaining_width):
        starting_width = remaining_width

        candidate_titles = get_candidate_titles(selected_titles, title_to_format, is_first_pass)
        is_first_pass = False

        maybe_select_title(candidate_titles, selected_titles, title_to_format, remaining_width, separation_width)

    def title_key_fn(s: str) -> int:
        return list(title_to_format.keys()).index(s)

    title_to_specifier: Specifiers = OrderedDict()

    sorted_titles = sorted(selected_titles, key=title_key_fn)
    for t in sorted_titles:
        f = title_to_format[t]
        k = f.key
        s = f.getSpecifier(t, f.getAlignment())
        title_to_specifier[t] = KeyAndSpecifier(k, s)

    return title_to_specifier
