"""Helper functions for cleaning MM data before inserting into database."""

from datetime import timezone

from geopandas import GeoDataFrame
from pandas import DataFrame
from pandas import melt as pd_melt
from pandas import merge as pd_merge
from pandas import to_datetime


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
    rq_cols = ["world_utm_id", "cell_id", "lat", "lon"]
    msg = f"The following columns must be in `gdf_request`: {rq_cols}"
    assert set(rq_cols).issubset(gdf_request.columns), msg

    df_wx_in = df_wx.copy()

    gdf_clean = gdf_request[rq_cols].drop_duplicates()

    # add cell ID back to MM data
    df_wx_full = pd_merge(
        df_wx_in.reset_index(),
        gdf_clean,
        how="left",
        on=["lat", "lon"],
    )

    # adjust validdate column to aware of `utc_offset` before converting to date
    df_wx_full["date"] = df_wx_full["validdate"].dt.tz_convert(utc_offset).dt.date

    # finalize columns
    df_wx_clean = df_wx_full.drop(
        columns=[
            "lat",
            "lon",
            "validdate",
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
    """Filters out data based on temporal bounds given by `gdf_request`.

    This filtering approach depends on the columns in `gdf_request`:
    - If "date_first" and "date_last" available, uses those columns as temporal bounds.
    - If "date" and "parameter" available, uses those columns to directly match with those in `df_clean`

    Args:
        df_clean (DataFrame): Cleaned MM weather (i.e., passed through `clean_meteomatics_data()`)
        gdf_request (GeoDataFrame): Full `gdf` that was used to generate the requests for `df_clean`
    """
    if set(["date_first", "date_last"]).issubset(gdf_request.columns):
        df_combined = pd_merge(df_clean, gdf_request, on="cell_id")
        keep = (
            to_datetime(df_combined["date"]) >= df_combined["date_first"].dt.date
        ) * (to_datetime(df_combined["date"]) <= df_combined["date_last"])
        return df_clean.loc[keep]

    else:
        df_combined = pd_merge(
            df_clean,
            gdf_request.drop(columns=["world_utm_id"]),
            left_on=["cell_id", "date", "variable"],
            right_on=["cell_id", "date", "parameter"],
        )
        return df_combined[df_clean.columns.values]
