"""Helper functions for cleaning MM data before inserting into database."""

from datetime import timezone

from geopandas import GeoDataFrame
from pandas import DataFrame
from pandas import melt as pd_melt
from pandas import merge as pd_merge


def clean_meteomatics_data(
    df_wx: DataFrame, gdf_request: GeoDataFrame, utc_offset: timezone
) -> DataFrame:
    """
    Cleans dataframe returned by Meteomatics request by adding cell ID back, unnesting indices, re-localizing date again, and melting to long format.

    Args:
        df_wx (pandas.DataFrame): Dataframe containing returned Meteomatics data for desired spatiotemporal AOI and parameters
        gdf_request (geopandas.GeoDataFrame): GeoDataFrame that maps cell ID to passed `lon` and `lat` values.
        utc_offset (datetime.timezone): UTC offset for needed UTM zone

    Returns:
        df_melt_clean (pandas.DataFrame): Cleaned Meteomatics data
    """

    df_wx_in = df_wx.copy()

    # add cell ID back to MM data
    df_wx_full = pd_merge(
        df_wx_in.reset_index(), gdf_request, how="left", on=["lat", "lon"]
    )

    # adjust validdate column to aware of `utc_offset` before converting to date
    df_wx_full["date"] = df_wx_full["validdate"].dt.tz_convert(utc_offset).dt.date

    # finalize columns
    df_wx_clean = df_wx_full.drop(
        columns=[
            "lat",
            "lon",
            "validdate",
            "centroid",
            "utm_zone",
            "utc_offset",
            "date_first",
            "date_last",
        ]
    )

    # melt wide dataframe to long format
    df_melt_clean = pd_melt(
        df_wx_clean, id_vars=["world_utm_id", "cell_id", "date", "date_requested"]
    )

    return df_melt_clean


def filter_meteomatics_data_by_gdf_bounds(
    df_clean: DataFrame, gdf_request: GeoDataFrame
) -> DataFrame:
    """Filters out data that is outside of ID's "date_first" and "date_last" in `gdf_request` from `df_clean`.

    Args:
        df_clean (DataFrame): Cleaned MM weather (i.e., passed through `clean_meteomatics_data()`)
        gdf_request (GeoDataFrame): Full `gdf` that was used to generate the requests for `df_clean`
    """

    df_combined = pd_merge(df_clean, gdf_request, on="cell_id")
    keep = (df_combined["date"] >= df_combined["date_first"]) * (
        df_combined["date"] <= df_combined["date_last"]
    )

    return df_clean.loc[keep]
