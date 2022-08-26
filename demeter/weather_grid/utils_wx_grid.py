from os.path import join
from typing import Tuple, Union

from geopandas import GeoDataFrame
from numpy import arange, count_nonzero, ndarray, uint32
from numpy.typing import ArrayLike, DTypeLike
from rasterio import open as rio_open
from shapely.geometry import MultiPolygon, Point, Polygon


def assign_cell_ids(
    array_mask: ArrayLike,
    mask_valid: ArrayLike,
    cell_id_vector: ArrayLike,
    dtype_out: DTypeLike = uint32,
) -> ndarray:
    array_cell_id = array_mask.copy()
    array_cell_id[
        mask_valid
    ] = cell_id_vector  # assign cell_ids to all valid pixels (in order)
    array_cell_id[~mask_valid] = 0  # assign all invalid/null pixels a value of 0
    return array_cell_id.astype(dtype_out)


def determine_distance(
    geometry_utm: Polygon,
    origin_m: Tuple[float, float],
    hemisphere_ew: str,
    hemisphere_ns: str,
    pole: bool,
) -> Tuple[float, float]:
    minx_m, miny_m, maxx_m, maxy_m = geometry_utm.bounds

    if pole and hemisphere_ew == "west":  # origin is maxx, miny
        dist_x_m = origin_m[0] - minx_m
        dist_y_m = maxy_m - origin_m[1]
    elif pole and hemisphere_ew == "east":  # origin is minx, miny
        dist_x_m = maxx_m - origin_m[0]
        dist_y_m = maxy_m - origin_m[1]
    elif hemisphere_ns == "south":  # origin is minx, maxy
        dist_x_m = maxx_m - origin_m[0]
        dist_y_m = origin_m[1] - miny_m
    elif hemisphere_ns == "north":  # origin is minx, miny
        dist_x_m = maxx_m - origin_m[0]
        dist_y_m = maxy_m - origin_m[1]
    else:
        raise ValueError("hemisphere_ew or hemisphere_ns are not set.")
    return dist_x_m, dist_y_m


def determine_origin(
    geometry_dd: Polygon,
    geometry_utm: Polygon,
    hemisphere_ew: str,
    hemisphere_ns: str,
    pole: bool,
) -> Tuple[float, float]:

    minx_d, miny_d, maxx_d, maxy_d = geometry_dd.bounds
    minx_m, miny_m, maxx_m, maxy_m = geometry_utm.bounds

    if pole and hemisphere_ew == "west":  # origin is maxx, miny
        origin_d = (minx_d, maxy_d)  # have to flip from utm because of 32761 CRS
        origin_m = (maxx_m, miny_m)
    elif pole and hemisphere_ew == "east":  # origin is minx, miny
        origin_d = (minx_d, miny_d)
        origin_m = (minx_m, miny_m)
    elif hemisphere_ns == "south":  # origin is minx, maxy
        origin_d = (minx_d, maxy_d)
        origin_m = (minx_m, maxy_m)
    elif hemisphere_ns == "north":  # origin is minx, miny
        origin_d = (minx_d, miny_d)
        origin_m = (minx_m, miny_m)
    else:
        raise ValueError("hemisphere_ew or hemisphere_ns are not set.")
    return origin_d, origin_m


def determine_transform(
    hemisphere_ew: str, hemisphere_ns: str, pole: bool
) -> Tuple[int, int]:
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


def increment_cell_ids(
    cell_id_min: int, cell_id_max: int, array_mask: ArrayLike
) -> Tuple[ndarray, int, int]:
    cell_id_min = cell_id_max + 1

    valid_pixel_n = count_nonzero(array_mask == 0)  # count valid pixels
    cell_id_max = cell_id_min + valid_pixel_n - 1  # calculate max cell_id for raster
    cell_id_vector = arange(cell_id_min, cell_id_max + 1)
    return cell_id_vector, cell_id_min, cell_id_max


def get_cell_id(
    base_dir_wx_grid: str,
    utm_row: str,
    utm_zone: int,
    geometry: Union[MultiPolygon, Point, Polygon],
    geometry_epsg: int = 4326,
) -> int:
    """Load appropriate raster based on zone and row."""
    geometry = (
        geometry.centroid if isinstance(geometry, (Polygon, MultiPolygon)) else geometry
    )
    with rio_open(
        join(
            base_dir_wx_grid,
            "5km_weather_grid",
            f"row-{utm_row}_zone-{int(utm_zone)}_5km.tif",
        ),
        "r",
    ) as ds:
        geom_pt = (
            GeoDataFrame(crs=geometry_epsg, geometry=[geometry])
            .to_crs(ds.crs.to_epsg())
            .iloc[0]["geometry"]
        )
        # geom_pt = gdf_pt.iloc[0]["geometry"]
        return [x[0] for x in ds.sample([[geom_pt.x, geom_pt.y]])][0]
