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
    """Split `gdf` into separate requests for each UTM zone appearing within the dataframe.."""
    list_gdf = []
    df_zones = gdf[["utm_zone"]].drop_duplicates()
    for _, row in df_zones.iterrows():
        gdf_zone_subset = gdf.loc[gdf["utm_zone"] == row["utm_zone"]]
        list_gdf += [gdf_zone_subset[GDF_COLS]]
    return list_gdf


def split_by_n_cells(gdf: GeoDataFrame, max_n_cells: int) -> List[GeoDataFrame]:
    """Split `gdf` evenly into N requests such that no request has more than `max_n_cells`."""
    list_gdf = []
    n_splits = int(ceil(len(gdf) / max_n_cells))
    list_gdf = array_split(gdf, n_splits)
    return list_gdf


def split_by_year(gdf: GeoDataFrame, n_forecast_days: int = 7) -> List[GeoDataFrame]:
    """Split `gdf` into one request per year for all years that fall within at least one cell IDs
    temporal bounds.

    This function assumes that all dates are required for all years that a cell ID needs. For the current
    year, this means up until today UTC + `n_forecast_days` days.

    Args:
        gdf (GeoDataFrame): GeoDataFrame to split
        n_forecast_days (int): Number of days to forecast from today (UTC)
    """
    list_gdf = []

    gdf["year_first"] = gdf["date_first"].dt.year
    gdf["year_last"] = to_datetime(gdf["date_last"]).dt.year

    first_year = gdf["year_first"].min()
    last_year = gdf["year_last"].max()
    years = range(first_year, last_year + 1)

    for year in years:
        gdf_year_subset = gdf.loc[gdf["year_first"] <= year]
        gdf_year_subset = gdf_year_subset.loc[gdf["year_last"] >= year]

        gdf_year_subset["date_first"] = datetime(year, 1, 1)

        if year == datetime.now().year:
            last_date_current_year = datetime.now(tz=UTC) + timedelta(
                days=n_forecast_days
            )
            gdf_year_subset["date_last"] = datetime(
                last_date_current_year.year,
                last_date_current_year.month,
                last_date_current_year.day,
            )
        else:
            gdf_year_subset["date_last"] = datetime(year, 12, 31)

        list_gdf += [gdf_year_subset[GDF_COLS]]

    return list_gdf
