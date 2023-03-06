"""Methods for splitting up requests"""
from datetime import datetime, timedelta
from typing import List

from geopandas import GeoDataFrame
from numpy import array_split, ceil
from pandas import to_datetime
from pytz import UTC

GDF_COLS = [
    "utm_zone",
    "utc_offset",
    "world_utm_id",
    "cell_id",
    "centroid",
    "date_first",
    "date_last",
]


def split_by_utm_zone(gdf: GeoDataFrame) -> List[GeoDataFrame]:
    """DOCSTRING"""
    list_gdf = []
    df_zones = gdf[["utm_zone"]].drop_duplicates()
    for _, row in df_zones.iterrows():
        gdf_zone_subset = gdf.loc[gdf["utm_zone"] == row["utm_zone"]]
        list_gdf += [gdf_zone_subset[GDF_COLS]]
    return list_gdf


def split_by_n_cells(gdf: GeoDataFrame, max_n_cells: int) -> List[GeoDataFrame]:
    """DOCSTRING"""
    list_gdf = []
    n_splits = int(ceil(len(gdf) / max_n_cells))
    list_gdf = array_split(gdf, n_splits)
    return list_gdf


def split_by_year(
    gdf: GeoDataFrame, date_bound_method: str = "all_possible", n_forecast_days: int = 7
) -> List[GeoDataFrame]:
    """Split `gdf` into one request for each year as needed for temporal bounds.

    Two methods for splitting:
    1. "all_possible" assumes that we want all dates for a given year if the year is within the
    temporal bounds of `gdf`. This means Jan 1 to Dec 31 (or until `n_forecast_days` after today's date
    for current year).
    2. "all_needed" determines temporal bounds based on what is explicitly requested in `gdf`.

    DOCSTRING

    Args:
        gdf (GeoDataFrame):
        date_bound_m
    """

    assert date_bound_method in [
        "all_possible",
        "all_needed",
    ], '`date_last_method` must be "all_possible" or "all_needed"'

    list_gdf = []

    gdf["year_first"] = gdf["date_first"].dt.year

    gdf["year_last"] = to_datetime(gdf["date_last"]).dt.year

    first_year = gdf["year_first"].min()
    last_year = gdf["year_last"].max()
    years = range(first_year, last_year + 1)

    for year in years:
        gdf_year_subset = gdf.loc[gdf["year_first"] <= year]
        gdf_year_subset = gdf_year_subset.loc[gdf["year_last"] >= year]

        if date_bound_method == "all_possible":
            gdf_year_subset["date_first"] = datetime(year, 1, 1)

            if year == datetime.now().year:
                gdf_year_subset["date_last"] = datetime.now(tz=UTC).replace(
                    hour=0, minute=0, second=0
                ) + timedelta(days=n_forecast_days)
            else:
                gdf_year_subset["date_last"] = datetime(year, 12, 31)

        else:
            date_first_subset = gdf_year_subset["date_first"].min()
            gdf_year_subset["date_first"] = max(
                [date_first_subset, datetime(year, 1, 1)]
            )

            date_last_subset = gdf_year_subset["date_last"].max()
            gdf_year_subset["date_last"] = min(
                [date_last_subset, datetime(year, 12, 31)]
            )

        list_gdf += [gdf_year_subset[GDF_COLS]]

    return list_gdf


# def
