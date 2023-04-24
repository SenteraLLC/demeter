# %%
from ast import literal_eval
from os.path import (
    dirname,
    join,
    realpath,
)

import pyproj
import rasterio
from dotenv import load_dotenv
from geo_utils.vector import reproject_shapely
from geopandas import GeoDataFrame
from rasterio.features import shapes
from rasterio.io import MemoryFile

from demeter.db import getConnection


def get_raster_layer_as_geojson(utm_row: str, utm_zone: int) -> GeoDataFrame:
    _ = load_dotenv()

    conn = getConnection("DEMETER-DEV_LOCAL_SUPER")
    with conn.connection.cursor() as cursor:
        cursor.execute("SET postgis.enable_outdb_rasters = True;")
        cursor.execute("SET postgis.gdal_enabled_drivers = 'ENABLE_ALL';")

        stmt = """SELECT ST_AsGDALRaster(st_union(rast_cell_id), 'GTIFF') FROM raster_5km r
        CROSS JOIN world_utm w
        WHERE w.zone = %(utm_zone)s AND w.row = %(utm_row)s
        AND w.world_utm_id = r.world_utm_id"""
        args = {"utm_zone": int(utm_zone), "utm_row": utm_row}

        cursor.execute(stmt, args)
        result = cursor.fetchone()

        stmt = """SELECT rast_metadata FROM raster_5km r
        CROSS JOIN world_utm w
        WHERE w.zone = %(utm_zone)s AND w.row = %(utm_row)s
        AND w.world_utm_id = r.world_utm_id"""
        args = {"utm_zone": int(utm_zone), "utm_row": utm_row}

        cursor.execute(stmt, args)
        profile = cursor.fetchone().rast_metadata

    # Prepare raster
    memfile = MemoryFile(bytes(result[0]))

    profile["crs"] = pyproj.Proj(f"+init={profile['crs']}")
    profile["dtype"] = "int32"
    profile["transform"] = literal_eval(profile["transform"])
    rst = memfile.open(**profile)

    # Make into features
    mask = None
    with rasterio.Env():
        image = rst.read(1)
        image2 = image.astype("int32")
        results = (
            {"properties": {"raster_val": v}, "geometry": s}
            for i, (s, v) in enumerate(
                shapes(image2, mask=mask, transform=rst.transform)
            )
        )
    geoms = list(results)

    # Make into GeoDataFrame
    gdf = GeoDataFrame.from_features(geoms)
    gdf.set_crs(profile["crs"].crs.to_epsg(), inplace=True)

    geom_reproject = [
        reproject_shapely(gdf.crs.to_epsg(), 4326, g) for g in gdf.geometry
    ]

    gdf["geometry"] = geom_reproject
    gdf["cell_id"] = gdf["raster_val"].astype(int)

    # Save to file and make into JavaScript
    file_dir = realpath(join(dirname(__file__), ".."))
    file_name = f"{file_dir}/data/raster.geojson"
    js_file_name = f"{file_dir}/data/raster.js"
    gdf.drop(columns="raster_val").to_file(file_name, driver="GeoJSON")
    with open(file_name, "r") as f:
        content = f.read()
    with open(js_file_name, "w") as f:
        f.write("var RASTER = ")
        f.write(content)


# %%
get_raster_layer_as_geojson(utm_row="U", utm_zone=12)
