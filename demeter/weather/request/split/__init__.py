"""Split functions (i.e., creating request lists) and request functions in step (2) of weather process."""
from ._split_gdf import (
    split_gdf_for_add,
    split_gdf_for_fill,
    split_gdf_for_update,
)

__all__ = ["split_gdf_for_add", "split_gdf_for_update", "split_gdf_for_fill"]
