# %% 1. Imports
from geopandas import read_file as gpd_read_file
from numpy import arange, count_nonzero, uint32, uint64
from os.path import join
from pandas import DataFrame
from pandas import concat as pd_concat
from pyproj import CRS
from rasterio import Affine, MemoryFile
from rasterio.mask import mask as rio_mask
from rasterio.profiles import DefaultGTiffProfile

from geo_utils.general import estimate_utm_crs, hemisphere_from_centroid
from geo_utils.raster import (
    create_array_skeleton,
    build_transform_utm,
    dec_deg_from_meters,
    save_rasterio,
)
from geo_utils.vector import reproject_shapely

from demeter.weather_grid.utils_wx_grid import (
    assign_cell_ids,
    determine_distance,
    determine_origin,
    determine_transform,
    increment_cell_ids,
)

# %% 2. Load UTM layer and set constants

base_dir = "/mnt/d/weather_grid"

gdf_utm = (
    gpd_read_file(join(base_dir, "utm_grid.geojson"))
    .sort_values(["row", "zone"])
    .reset_index(drop=True)
)
# ).explode(index_parts=False)

epsg_wgs = 4326
pix_size_m = 5000
crs_wgs = CRS.from_epsg(epsg_wgs)
cell_id_min, cell_id_max = 0, 0
dtype_template, dtype_out = uint64, uint32
nodata_mask_valid = 999
cols = [
    "row",
    "zone",
    "width",
    "height",
    "valid_pixel_n",
    "null_pixel_n",
    "cell_id_min",
    "cell_id_max",
]
df_rasters = DataFrame([], columns=cols)

# %% 3. Loop
# utm_poly = gdf_utm.loc[2]  # this is

for idx, utm_poly in gdf_utm.iterrows():
    # if idx > 8:
    #     break
    # a. Get basic geometric information
    row, zone = utm_poly.row, int(utm_poly.zone)

    centroid = (utm_poly.geometry.centroid.x, utm_poly.geometry.centroid.y)
    hemisphere_ew, hemisphere_ns, pole = hemisphere_from_centroid(centroid)

    crs_utm = estimate_utm_crs(centroid[0], centroid[1])
    utm_poly_utm = reproject_shapely(
        epsg_src=epsg_wgs, epsg_dst=crs_utm.to_epsg(), geometry=utm_poly.geometry
    )

    origin_d, origin_m = determine_origin(
        geometry_dd=utm_poly.geometry,
        geometry_utm=utm_poly_utm,
        hemisphere_ew=hemisphere_ew,
        hemisphere_ns=hemisphere_ns,
        pole=pole,
    )
    transform_coef_x, transform_coef_y = determine_transform(
        hemisphere_ew=hemisphere_ew, hemisphere_ns=hemisphere_ns, pole=pole
    )
    dist_x_m, dist_y_m = determine_distance(
        geometry_utm=utm_poly_utm,
        origin_m=origin_m,
        hemisphere_ew=hemisphere_ew,
        hemisphere_ns=hemisphere_ns,
        pole=pole,
    )

    # b. Calculate transforms
    transform_utm = build_transform_utm(
        origin_m, pix_size_m, transform_coef_x, transform_coef_y
    )
    transform_a, transform_e = dec_deg_from_meters(
        transform_utm, epsg_utm=crs_utm.to_epsg(), epsg_wgs=4326
    )
    transform_wgs = Affine(
        abs(transform_a) * transform_coef_x * -1,  # -1 because ?
        0,
        origin_d[0],
        0,
        abs(transform_e) * transform_coef_y * -1,
        origin_d[1],
    )

    # c. Create array skeleton and profiles
    array = create_array_skeleton(  # Calculate number of pixels and create array
        dist_x_m, dist_y_m, pix_size_m=pix_size_m, dtype=dtype_template
    )
    profile = DefaultGTiffProfile(
        count=1,
        width=array.shape[-2],
        height=array.shape[-1],
        dtype=dtype_template,
        crs=crs_utm,
        transform=transform_utm,
    )
    profile_wgs = DefaultGTiffProfile(
        count=1,
        width=array.shape[-2],
        height=array.shape[-1],
        dtype=dtype_template,
        crs=crs_wgs,
        transform=transform_wgs,
    )
    if pole:
        profile_wgs.update(
            width=array.shape[-1], height=array.shape[-2]
        )  # have to flip these too!

    # d. Create valid mask
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

    # e. Fill in array with cell_ids and get stats
    cell_id_vector, cell_id_min, cell_id_max = increment_cell_ids(
        cell_id_min, cell_id_max, array_mask
    )
    array_cell_id = assign_cell_ids(array_mask, mask_valid, cell_id_vector, dtype_out)
    valid_pixel_n = count_nonzero(array_cell_id > 0)
    null_pixel_n = count_nonzero(array_cell_id == 0)  # count null

    # Save raster and stats to file
    profile.update(dtype=array_cell_id.dtype)
    save_rasterio(
        dir_out=join(base_dir, "5km_weather_grid_local"),
        fname_out=f"row-{row}_zone-{zone}_5km.tif",
        arr=array_cell_id,
        profile=profile,
        band_description=["cell_id_local"],
    )

    df_temp = DataFrame(
        [
            [
                row,
                zone,
                profile["width"],
                profile["height"],
                valid_pixel_n,
                null_pixel_n,
                cell_id_min,
                cell_id_max,
            ]
        ],
        columns=df_rasters.columns,
    )
    df_rasters = pd_concat([df_rasters, df_temp], axis=0, ignore_index=True)
df_rasters.to_csv(join(base_dir, "5km_weather_grid_metadata.csv"), index=False)


if __name__ == "__main__":
    pass

# %% Visualization only (???)
# from matplotlib import pyplot as plt
# from numpy import flip as np_flip

# if pole and hemisphere_ew == "west" and hemisphere_ns == "south":
#     flip_x = True
#     flip_y = True
# elif pole and hemisphere_ew == "east" and hemisphere_ns == "south":
#     flip_x = False
#     flip_y = True
# elif pole and hemisphere_ew == "west" and hemisphere_ns == "north":
#     flip_x = True
#     flip_y = False
# elif pole and hemisphere_ew == "east" and hemisphere_ns == "north":
#     flip_x = False
#     flip_y = False

# if flip_x is True:
#     array_cell_id = np_flip(array_cell_id, axis=-2)
# if flip_y is True:
#     array_cell_id = np_flip(array_cell_id, axis=-1)


# plt.imshow(array_cell_id[0, :, :])


# For each utm polygon:
# 0. The origin of the poles should be treated different than the rest; origin should be:
#    - Row A (South pole): maxx, miny (bottom right)
#    - Row B (South pole): minx, miny (bottom left)
#    - Row Y (North pole): maxx, miny (bottom right)
#    - Row Z (North pole): minx, miny (bottom left)
# 1. Determine lat/lng origin (minx, maxy/miny). Origin y should be point nearest equator.
# 2. Calculate distance in meters from origin to maxx and orign to y of opposite corner of utm polygon.
# 2. Calculate the lng that is a multiple of 5km and greater than maxx.
# 3. Calculate the lat that is a multiple of 5km and greater than maxy.
# 4. Using maxx_5km and maxy_5km as the northeast extent, generate the 5km grid raster with extent from origin to (maxx, maxy).
