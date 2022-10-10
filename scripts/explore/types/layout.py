from ..formatting import ColumnFormat, FormatOptions

from collections import OrderedDict

LAYOUT = OrderedDict(
                (("Type Name",
                  ColumnFormat("type_name",
                               FormatOptions(max_columns=30),
                              ),
                 ),
                 ("Category Name",
                  ColumnFormat("type_category",
                               FormatOptions(is_required=False, max_columns=20),
                              ),
                 ),
                 ("Value Count",
                  ColumnFormat("value_count",
                               FormatOptions(max_columns=10, align=1),
                              ),
                 ),

                 ("Unit to Count",
                   ColumnFormat("unit_to_count",
                                FormatOptions(is_required=False, max_columns=100),
                               ),
                 ),
                 ("Units",
                  ColumnFormat("units",
                               FormatOptions(is_required=False, max_columns=40),
                              ),
                 ),
                 ("Unit Count",
                  ColumnFormat("unit_count",
                               FormatOptions(max_columns=10, align=1, xor_title="Units"),
                              ),

                 ),
                 ("Groups",
                  ColumnFormat("groups",
                               FormatOptions(is_required=False, max_columns=100),
                              ),
                 ),
                 ("Group Count",
                  ColumnFormat("group_count",
                               FormatOptions(is_required=False, max_columns=10, align=1, xor_title="Groups"),
                              ),
                 ),
               )
             )


