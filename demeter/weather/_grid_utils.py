import json
import subprocess
from datetime import timezone
from os.path import isfile
from tempfile import NamedTemporaryFile
from typing import Any, Tuple

from geo_utils.general import estimate_utm_crs, hemisphere_from_centroid
from geo_utils.raster import build_transform_utm, create_array_skeleton
from geo_utils.vector import reproject_shapely
from geo_utils.world import round_coordinate
from numpy import (
    arange,
    count_nonzero,
    ndarray,
    uint32,
    uint64,
)
from numpy.typing import ArrayLike, DTypeLike
from pandas import DataFrame, Series
from pyproj import CRS
from rasterio import MemoryFile
from rasterio import open as rio_open
from rasterio.mask import mask as rio_mask
from rasterio.profiles import DefaultGTiffProfile
from shapely.geometry import Point, Polygon
from shapely.wkb import loads as wkb_loads
from sqlalchemy.engine import Connection


def determine_raster_origin(
    geometry_dd: Polygon,
    geometry_utm: Polygon,
    hemisphere_ew: str,
    hemisphere_ns: str,
    pole: bool,
) -> Tuple[Tuple[float], Tuple[float]]:
    """Determines raster origin for WGS- and UTM-projected UTM polygons.

    Raster origin should be on the latitudinal axis nearest the equator.

    For southern hemisphere, this means that the origin is at the NW point.
    For northern hemisphere, this means that the origin is at the SW point.
    The poles are handled as special cases.

    Args:
        geometry_dd (shapely.Polygon): UTM polygon in WGS-84
        geometry_utm (shapely.Polygon): UTM polygon projected in estimated UTM CRS
        hemisphere_ew (str): whether UTM polygon is on "east" or "west" hemisphere
        hemisphere_ns (str): whether UTM polygon is on "north" or "south" hemisphere
        pole (bool): whether UTM polygon is one of the poles

    Returns origin_d, origin_m
        origin_dd (tuple(float, float)): Origin in WGS-84
        origin_utm (tuple(float, float)): Origin in projected UTM CRS
    """

    minx_d, miny_d, _, maxy_d = geometry_dd.bounds
    minx_m, miny_m, maxx_m, maxy_m = geometry_utm.bounds

    if pole and hemisphere_ew == "west":  # origin is maxx, miny
        origin_dd = (minx_d, maxy_d)  # have to flip from utm because of 32761 CRS
        origin_utm = (maxx_m, miny_m)
    elif pole and hemisphere_ew == "east":  # origin is minx, miny
        origin_dd = (minx_d, miny_d)
        origin_utm = (minx_m, miny_m)
    elif hemisphere_ns == "south":  # origin is minx, maxy
        origin_dd = (minx_d, maxy_d)
        origin_utm = (minx_m, maxy_m)
    elif hemisphere_ns == "north":  # origin is minx, miny
        origin_dd = (minx_d, miny_d)
        origin_utm = (minx_m, miny_m)
    else:
        raise ValueError("hemisphere_ew or hemisphere_ns are not set.")
    return origin_dd, origin_utm


def determine_transform(
    hemisphere_ew: str, hemisphere_ns: str, pole: bool
) -> Tuple[int, int]:
    """Determine whether X, Y values should be added or subtracted when defining the raster.

    For example, if the raster origin starts at the top left corner of the polygon,
    Y values need to be decreased (transform_coef_y = -1) and X values need to be
    increased (transform_coef_x = 1) when defining intersections for the raster.
    """

    if pole and hemisphere_ew == "west":  # origin is maxx, miny
        transform_coef_x, transform_coef_y = (
            -1,
            1,
        )  # if origin_x is maxx (right), x must be negative
    elif pole and hemisphere_ew == "east":  # origin is minx, miny
        transform_coef_x, transform_coef_y = 1, 1
    elif hemisphere_ns == "south":  # origin is minx, maxy
        transform_coef_x, transform_coef_y = (
            1,
            -1,
        )  # if origin_y is maxy (top), y must be negative
    elif hemisphere_ns == "north":  # origin is minx, miny
        transform_coef_x, transform_coef_y = 1, 1
    else:
        raise ValueError("hemisphere_ew or hemisphere_ns are not set.")
    return transform_coef_x, transform_coef_y


def determine_distance(geometry_utm: Polygon) -> Tuple[float, float]:
    """Determines distance (meters) in the x and y direction of the UTM polygon.

    Args:
        geometry_utm (shapely.geometry.Polygon): UTM polygon projected in local UTM CRS

    Returns:
        dist_x_m (float): Distance (meters) represented by this UTM polygon along x direction
        dist_y_m (float): Distance (meters) represented by this UTM polygon along y direction
    """
    minx_m, miny_m, maxx_m, maxy_m = geometry_utm.bounds
    dist_x_m = maxx_m - minx_m
    dist_y_m = maxy_m - miny_m
    return dist_x_m, dist_y_m


def increment_cell_ids(
    cell_id_min: int, cell_id_max: int, array_mask: ArrayLike
) -> Tuple[ndarray, int, int]:
    """Determine first and last cell ID to be included in a UTM polygon raster.

    First ID is determined as previous `cell_id_max` + 1.
    Last ID is determined based on first ID and number of valid pixels.

    Args:
        cell_id_min (int): Previous UTM polygon first ID
        cell_id_max (int): Previous UTM polygon last ID
        array_mask (array): Mask for valid pixels in raster

    Returns:
        cell_id_vector (numpy.ndarray): 1xm array of cell IDs for this UTM polygon
        cell_id_min (int): Current UTM polygon first ID
        cell_id_max (int): Current UTM polygon last ID
    """
    cell_id_min = cell_id_max + 1

    valid_pixel_n = count_nonzero(array_mask == 0)  # count valid pixels
    cell_id_max = cell_id_min + valid_pixel_n - 1  # calculate max cell_id for raster
    cell_id_vector = arange(cell_id_min, cell_id_max + 1)
    return cell_id_vector, cell_id_min, cell_id_max


def assign_cell_ids(
    array_mask: ArrayLike,
    mask_valid: ArrayLike,
    cell_id_vector: ArrayLike,
    dtype_out: DTypeLike = uint32,
) -> ndarray:
    """Assign `cell_id_vector` contents to just valid pixels in array.

    Args:
        array_mask (array): Array providing valid pixels as 0 and invalid pixels as -999
        mask_valid (array): Array providing valid pixels as True and invalid pixels as False
        cell_id_vector (array): 1xM array providing cell IDs to be populated
        dtype_out (dtype): Data type for final array

    Returns array with cell IDs populated for valid pixels.
    """

    array_cell_id = array_mask.copy()
    array_cell_id[
        mask_valid
    ] = cell_id_vector  # assign cell_ids to all valid pixels (in order)
    array_cell_id[~mask_valid] = 0  # assign all invalid/null pixels a value of 0
    return array_cell_id.astype(dtype_out)


def create_raster_for_utm_polygon(utm_poly: Series, cell_id_min: int, cell_id_max: int):
    """Main function to create a weather grid raster for passed `utm_poly`.

    Args:
        utm_poly (GeoSeries): Row from GeoDataFrame loaded from `utm_grid.geojson`
        cell_id_min (int): First cell ID from previous polygon
        cell_id_max (int): Last cell ID from previous polygon

    Returns:
        array_cell_id (array): Array of cell IDs to map to raster
        profile (DefaultGTiffProfile): Metadata for raster
        cell_id_min (int); First cell ID for this polygon
        cell_id_max (int): Last cell ID for this polygon
    """
    epsg_wgs = 4326
    pix_size_m = 5000
    dtype_template, dtype_out = uint64, uint32
    nodata_mask_valid = 999

    # determine UTM CRS to use for this polygon based on centroid
    centroid = (utm_poly.geometry.centroid.x, utm_poly.geometry.centroid.y)
    hemisphere_ew, hemisphere_ns, pole = hemisphere_from_centroid(centroid)

    crs_utm = estimate_utm_crs(centroid[0], centroid[1])

    # reproject to UTM
    utm_poly_utm = reproject_shapely(
        epsg_src=epsg_wgs, epsg_dst=crs_utm.to_epsg(), geometry=utm_poly.geometry
    )

    # determine raster origin
    _, origin_utm = determine_raster_origin(
        geometry_dd=utm_poly.geometry,
        geometry_utm=utm_poly_utm,
        hemisphere_ew=hemisphere_ew,
        hemisphere_ns=hemisphere_ns,
        pole=pole,
    )

    # define the direction of travel when defining pixels from the origin
    transform_coef_x, transform_coef_y = determine_transform(
        hemisphere_ew=hemisphere_ew, hemisphere_ns=hemisphere_ns, pole=pole
    )

    # determine length of polygon sides
    dist_x_m, dist_y_m = determine_distance(geometry_utm=utm_poly_utm)

    # calculate affine transform for UTM projection from array indices
    transform_utm = build_transform_utm(
        origin_utm, pix_size_m, transform_coef_x, transform_coef_y
    )

    # calculate number of pixels and create array
    array = create_array_skeleton(
        dist_x_m, dist_y_m, pix_size_m=pix_size_m, dtype=dtype_template
    )

    # define the GeoTIFF metadata
    profile = DefaultGTiffProfile(
        count=1,
        width=array.shape[-2],
        height=array.shape[-1],
        dtype=dtype_template,
        crs=crs_utm,
        transform=transform_utm,
    )

    # create valid mask where pixels touch any part of UTM polygon
    with MemoryFile() as memfile, memfile.open(**profile) as ds:
        ds.write(array.astype(profile["dtype"]))
        array_mask, _ = rio_mask(
            ds,
            shapes=[utm_poly_utm],
            all_touched=True,
            nodata=nodata_mask_valid,
            filled=True,
        )
        mask_valid = array_mask == 0

    # get appropriate cell ID range for this UTM polygon
    cell_id_vector, cell_id_min, cell_id_max = increment_cell_ids(
        cell_id_min, cell_id_max, array_mask
    )
    array_cell_id = assign_cell_ids(array_mask, mask_valid, cell_id_vector, dtype_out)
    profile.update(dtype=array_cell_id.dtype)

    return array_cell_id, profile, cell_id_min, cell_id_max


def insert_utm_polygon(
    cursor: Any,
    zone: int,
    row: str,
    geom: Polygon,
    utc_offset: timezone,
    raster_epsg: int,
) -> None:
    """Insert UTM polygon into weather.world_utm table."""

    # insert
    stmt = """
    insert into world_utm(zone, row, geom, utc_offset, raster_epsg)
    values(
        %(zone)s, %(row)s, ST_MakeValid(%(geom)s::geometry), %(utc_offset)s, %(raster_epsg)s
    )
    """
    args = {
        "zone": zone,
        "row": row,
        "geom": geom.wkb_hex,
        "utc_offset": utc_offset,
        "raster_epsg": raster_epsg,
    }
    cursor.execute(stmt, args)


# TODO: Move to either geo_utils or `demeter_utils` to be used more broadly (?)
def add_raster(conn: Connection, array_cell_id: ArrayLike, profile: dict) -> None:
    """Insert weather grid raster into weather.raster_5km to start a new row."""

    host = conn.engine.url.host
    username = conn.engine.url.username
    password = conn.engine.url.password
    database = conn.engine.url.database
    port = conn.engine.url.port

    with NamedTemporaryFile(suffix=".tif") as tmpfile:
        with rio_open(tmpfile, "w", **profile) as rast_mem:
            rast_mem.write(array_cell_id)
            rast_mem.set_band_description(1, "cell_id")
            rast_mem.update_tags(**profile)
            print(tmpfile.name)
            print(isfile(tmpfile.name))

            epsg = rast_mem.crs.to_epsg()

        raster2pgsql = (
            "raster2pgsql -f 'rast_cell_id' -a -s {0} -N 0 {1} "
            "{2}.{3}".format(
                epsg,
                tmpfile.name,
                "weather",
                "raster_5km",
            )
        )

        psql = (
            f"PGPASSWORD={password} psql -d {database} -U {username} "
            f"-h {host} -p {port} -w"
        )

        subprocess.run("{0} | {1}".format(raster2pgsql, psql), shell=True)


def add_rast_metadata(cursor: Any, raster_5km_id: int, profile: dict) -> None:
    """Add raster metadata to complete database entry in weather.raster_5km for `raster_5km_id`."""

    profile_dict = {
        k.replace(" ", "_") if " " in k else k: v for k, v in profile.items()
    }
    profile_dict["crs"] = profile_dict["crs"].to_string()
    profile_dict["transform"] = json.dumps(profile["transform"].to_gdal())
    profile_dict["dtype"] = str(profile_dict["dtype"])
    profile_json = json.dumps(profile_dict)

    # update entry for raster that was just inserted
    stmt = """
    update raster_5km
    set world_utm_id = %(raster_5km_id)s, rast_metadata = %(profile)s
    where raster_5km_id = %(raster_5km_id)s;
    """
    args = {
        "raster_5km_id": raster_5km_id,
        "profile": profile_json,
    }
    cursor.execute(stmt, args)


def get_cell_id(
    cursor: Any, geometry: Point, geometry_crs: CRS = CRS.from_epsg(4326)
) -> Series:
    """For a given Point geometry, determine cell ID and cell centroid.

    If the point geometry somehow falls on a polygon boundary, which results in 2 cell IDs
    being returned, the smaller cell ID will always be returned for consistency.


    Args:
        cursor (Any): Connection to Demeter weather database
        geometry (shapely.geometry.Point): Point geometry for which to extract cell ID

        geometry_crs (pyproj.CRS): Coordinate reference system for `geometry`. Defaults to
            WGS 84 (EPSG=4326).

    Returns:
        cell ID (int) of weather grid pixel the point falls within
        centroid (shapely.Point) of pixel with lat/lng values rounded to 5 decimal places
    """

    assert isinstance(geometry, Point), "`geometry` must be passed as a `Point`"

    epsg_src = geometry_crs.to_epsg()
    if epsg_src != 4326:
        epsg_dst = 4326
        geometry = reproject_shapely(
            epsg_src=epsg_src, epsg_dst=epsg_dst, geometry=geometry
        )

    stmt = """
    select q2.cell_id, ST_ReducePrecision(ST_Transform(ST_PixelAsCentroid(q2.rast, q2.x, q2.y), 4326), 0.00001) as centroid
    from (
        select q.cell_id as cell_id, q.rast, (ST_PixelOfValue(q.rast, 1, q.cell_id)).*
        from (
            select raster_5km.rast_cell_id as rast,
                ST_Value(
                raster_5km.rast_cell_id,
                ST_Transform(ST_Point( %(x)s, %(y)s, 4326), world_utm.raster_epsg)
                ) as cell_id
            from world_utm, raster_5km
            where ST_intersects(ST_Point(%(x)s, %(y)s, 4326), world_utm.geom)
            and world_utm.world_utm_id=raster_5km.world_utm_id
        ) as q
    ) as q2;
    """
    args = {"x": geometry.x, "y": geometry.y}

    cursor.execute(stmt, args)
    res = DataFrame(cursor.fetchall()).sort_values(by=["cell_id"])

    # if more than one cell ID returned (i.e., on polygon edge), take the smaller cell ID arbitrarily
    full_pt = wkb_loads(res.at[0, "centroid"], hex=True)
    centroid = Point(round_coordinate([full_pt.x, full_pt.y], 5))

    return Series(data={"cell_id": int(res.at[0, "cell_id"]), "centroid": centroid})
