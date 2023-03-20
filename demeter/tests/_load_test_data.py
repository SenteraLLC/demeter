# %% Imports
from datetime import datetime

from aws_utils.s3_utils import list_s3_urls, role_to_s3
from dotenv import load_dotenv
from geo_utils.world import round_gdf_geometries, round_geometry
from geopandas import read_file
from pandas import concat as pd_concat
from pandas import isnull as pd_isnull

from demeter.data import (
    Act,
    CropType,
    Field,
    FieldGroup,
    insertOrGetAct,
    insertOrGetCropType,
    insertOrGetField,
    insertOrGetFieldGroup,
    insertOrGetGeom,
)
from demeter.db import getConnection

if __name__ == "__main__":
    c = load_dotenv()
    conn = getConnection(env_name="DEMETER-DEV_LOCAL")
    cursor = conn.connection.cursor()

    # Data urls
    s3_resource = role_to_s3(None)
    s3_bucket_name = "sentera-demeter-test-data"
    s3_prefix = "tylernigon/2022"
    nigon_urls = list_s3_urls(s3_resource, s3_bucket_name, s3_prefix)

    field_bounds = read_file(
        nigon_urls[[idx for idx, s in enumerate(nigon_urls) if "field-bounds" in s][0]]
    )
    as_planted = read_file(
        nigon_urls[[idx for idx, s in enumerate(nigon_urls) if "as-planted" in s][0]]
    )

    field_bounds = round_gdf_geometries(field_bounds, n_decimal_places=7)
    as_planted = round_gdf_geometries(as_planted, n_decimal_places=7)

    # Add data to demeter - FieldGroups and Fields
    trans = conn.begin()
    field_group_owner = FieldGroup(
        name="Tyler Nigon",
        parent_field_group_id=None,
        details={"field_group_type": "owner"},
    )
    owner_group_id = insertOrGetFieldGroup(cursor, field_group_owner)

    for owner in field_bounds["owner"].unique():
        field_group_grower = FieldGroup(
            name=owner,
            parent_field_group_id=owner_group_id,
            details={"field_group_type": "grower"},
        )
        grower_group_id = insertOrGetFieldGroup(cursor, field_group_grower)
        for farm in field_bounds[field_bounds["owner"] == owner]["farm"].unique():
            field_group_farm = FieldGroup(
                name=farm,
                parent_field_group_id=grower_group_id,
                details={"field_group_type": "farm"},
            )
            farm_group_id = insertOrGetFieldGroup(cursor, field_group_farm)
            for i, row_fb in field_bounds[field_bounds["farm"] == farm].iterrows():
                # geom_fb = round_geometry(row_fb["geometry"], n_decimal_places=7)
                geom_fb = row_fb["geometry"]
                field_geom_id = insertOrGetGeom(cursor, geom_fb)
                field = Field(
                    name=row_fb["field_id"],
                    geom_id=field_geom_id,
                    date_start=datetime(row_fb["year"] - 1, 12, 1),
                    # date_end=datetime(row["year"], 12, 1),
                    field_group_id=farm_group_id,
                )
                field_id = insertOrGetField(cursor, field)
                # A note on the date_end (and created) columns of Field
                #     1. `date_end=None` is NOT VALID because setting explicitly to `None` bypasses the default `datetime.max`
                #     2. When insertOrGetField() is called, date_end is assigned an infinity value because of the schema.sql default. I
                #        don't know how to reconcile the SQL default with the Field() class default if `None` is passed.
                #     So, DO NOT pass `None` to either the `date_end` or "Detailed" columns (i.e., `details`, `created`, `last_updated`).

                for i, row_ap in as_planted[
                    as_planted["field_id"] == row_fb["field_id"]
                ].iterrows():
                    geom_ap = round_geometry(row_ap["geometry"], n_decimal_places=7)
                    # RaiseException: Field geometry must cover act geometry.
                    if not geom_fb.covers(geom_ap):
                        print(
                            f'As-planted geometry is not covered by field boundary; interecting geometries for field "{row_ap["field_id"]}" index "{row_ap["index"]}"'
                        )
                        geom_ap = round_geometry(
                            row_ap["geometry"].intersection(geom_fb), n_decimal_places=7
                        )
                    # geom_ap = round_geometry(row_ap["geometry"].intersection(geom_fb), n_decimal_places=7)
                    # geom_ap = snap(geom_ap, geom_fb, tolerance=1e-6)

                    # getMaybeGeomId(cursor, geom_fb)
                    # getMaybeGeomId(cursor, geom_ap)

                    ap_geom_id = insertOrGetGeom(cursor, geom_ap)
                    crop = (
                        row_ap["crop"].lower()
                        if not pd_isnull(row_ap["crop"])
                        else None
                    )
                    product_name = (
                        row_ap["variety"].lower()
                        if not pd_isnull(row_ap["variety"])
                        else None
                    )
                    crop_type = CropType(crop=crop, product_name=product_name)
                    crop_type_id = insertOrGetCropType(cursor, crop_type)
                    date_performed = (
                        row_ap["date_plant"]
                        if not pd_isnull(row_ap["date_plant"])
                        else None
                    )
                    population = (
                        row_ap["population"]
                        if not pd_isnull(row_ap["population"])
                        else None
                    )
                    description = (
                        row_ap["description"]
                        if not pd_isnull(row_ap["description"])
                        else None
                    )

                    if date_performed is None:
                        continue
                    # if crop in ["alfalfa"] and date_performed is None:
                    #     date_performed = datetime(2021, 6, 1)  # Was probably seeded the previous crop season
                    # elif crop in ["winter wheat"] and date_performed is None:
                    #     date_performed = datetime(2021, 10, 15)
                    # elif crop in ["flowers", "mums", "pumpkins", "sweet corn"] and date_performed is None:
                    #     date_performed = datetime(2022, 6, 1)

                    planting = Act(
                        act_type="plant",
                        field_id=field_id,
                        date_performed=date_performed,
                        crop_type_id=crop_type_id,
                        geom_id=ap_geom_id,
                        details={
                            "seed_density_per_acre": population,
                            "description": description,
                        },
                    )
                    planting_id = insertOrGetAct(cursor, planting)

    trans.commit()
    trans.close()
