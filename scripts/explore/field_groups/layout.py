from ..formatting import ColumnFormat, FormatOptions

from collections import OrderedDict

LAYOUT = OrderedDict(
                (("Name",
                  ColumnFormat("name",
                               FormatOptions(max_columns=30),
                              ),
                 ),
                 ("Groups",
                  ColumnFormat("child_field_group_count",
                               FormatOptions(max_columns=10),
                              ),
                 ),
                 ("All Fields",
                  ColumnFormat("total_field_count",
                               FormatOptions(max_columns=10, align=1),
                              ),
                 ),
                 ("Direct Fields",
                  ColumnFormat("immediate_field_count",
                               FormatOptions(max_columns=10, align=1),
                              ),
                 ),
                 ("External Id",
                  ColumnFormat("external_id",
                               FormatOptions(max_columns=20, align=1),
                              ),
                 ),
               )
)
