from collections import OrderedDict

from ..formatting import ColumnFormat, FormatOptions

LAYOUT = OrderedDict(
    (
        (
            "Name",
            ColumnFormat(
                "name",
                FormatOptions(max_columns=30),
            ),
        ),
        (
            "All Groups",
            ColumnFormat(
                "total_group_count",
                FormatOptions(max_columns=10),
            ),
        ),
        (
            "Groups",
            ColumnFormat(
                "group_count",
                FormatOptions(max_columns=10),
            ),
        ),
        (
            "All Fields",
            ColumnFormat(
                "total_field_count",
                FormatOptions(max_columns=10, align=1),
            ),
        ),
        (
            "Fields",
            ColumnFormat(
                "field_count",
                FormatOptions(max_columns=10, align=1),
            ),
        ),
        (
            "External Id",
            ColumnFormat(
                "external_id",
                FormatOptions(max_columns=10, align=1),
            ),
        ),
    )
)
