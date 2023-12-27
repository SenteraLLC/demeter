# %% Imports
from datetime import datetime

from aws_utils.s3_utils import list_s3_urls, role_to_s3
from dotenv import load_dotenv
from geo_utils.world import round_gdf_geometries, round_geometry
from geopandas import read_file
from pandas import isnull as pd_isnull

from demeter.data import (
    Act,
    App,
    CropType,
    Field,
    Grouper,
    getMaybeGeom,
    insertOrGetAct,
    insertOrGetApp,
    insertOrGetCropType,
    insertOrGetField,
    insertOrGetGeom,
    insertOrGetGrouper,
)
from demeter.db import getConnection

if __name__ == "__main__":
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

    c = load_dotenv()
    conn = getConnection(env_name="DEMETER-DEV_LOCAL").connection

    with conn.cursor() as cursor:
        grouper_owner = Grouper(
            name="Tyler Nigon2",
            parent_grouper_id=None,
            details={"grouper_type": "owner"},
        )
        owner_grouper_id = insertOrGetGrouper(cursor, grouper_owner)
        for owner in field_bounds["owner"].unique():
            grouper_grower = Grouper(
                name=owner,
                parent_grouper_id=owner_grouper_id,
                details={"grouper_type": "grower"},
            )
            grower_grouper_id = insertOrGetGrouper(cursor, grouper_grower)
            for farm in field_bounds[field_bounds["owner"] == owner]["farm"].unique():
                grouper_farm = Grouper(
                    name=farm,
                    parent_grouper_id=grower_grouper_id,
                    details={"grouper_type": "farm"},
                )
                # if farm == "Nigon-View":
                #     break
                farm_grouper_id = insertOrGetGrouper(cursor, grouper_farm)
                for i, row_fb in field_bounds[field_bounds["farm"] == farm].iterrows():
                    # if row_fb["field_id"] == "01-12":
                    #     break
                    field_geom_id = insertOrGetGeom(cursor, row_fb["geometry"])
                    geom_fb = getMaybeGeom(cursor, geom_id=field_geom_id)
                    field = Field(
                        name=row_fb["field_id"],
                        geom_id=field_geom_id,
                        date_start=datetime(row_fb["year"] - 1, 12, 1),
                        # date_end=datetime(row["year"], 12, 1),
                        grouper_id=farm_grouper_id,
                    )
                    field_id = insertOrGetField(cursor, field)
                    # for i, row_ap in as_planted[as_planted["field_id"] == row_fb["field_id"]].iterrows():
                    #     # RaiseException: Field geometry must cover act geometry.
                    #     if not geom_fb.covers(row_ap["geometry"]):
                    #         print(
                    #             f'As-planted geometry is not covered by field boundary; interecting geometries for field "{row_ap["field_id"]}" index "{row_ap["index"]}"'
                    #         )
                    #         ap_geom_id = insertOrGetGeom(cursor, row_ap["geometry"].intersection(geom_fb))
                    #     else:
                    #         ap_geom_id = insertOrGetGeom(cursor, row_ap["geometry"])
                    #     geom_ap = getMaybeGeom(cursor, geom_id=ap_geom_id)

                    for i, row_ap in as_planted[
                        as_planted["field_id"] == row_fb["field_id"]
                    ].iterrows():
                        if (
                            len(
                                as_planted[as_planted["field_id"] == row_fb["field_id"]]
                            )
                            == 1
                        ):
                            # Assumes that if there is only one planting record, then it has same geometry as the field
                            geom_ap = getMaybeGeom(cursor, geom_id=field_geom_id)
                            ap_geom_id = insertOrGetGeom(cursor, geom_ap)
                            if field_geom_id != ap_geom_id:
                                raise RuntimeError(
                                    "Retrieved geometry from database does not match what got inserted! "
                                    "(getMaybeGeom() does not return the same thing that gets passed to "
                                    "insertOrGetGeom)."
                                )
                        else:
                            geom_ap = round_geometry(
                                row_ap["geometry"], n_decimal_places=7
                            )
                            if not geom_fb.buffer(1e-7).covers(
                                geom_ap
                            ):  # Would cause RaiseException: Field geometry must cover act geometry.
                                print(
                                    f"As-planted geometry is not covered by field boundary; interecting geometries for "
                                    f'field "{row_ap["field_id"]}" index "{row_ap["index"]}"'
                                )
                                geom_ap = round_geometry(
                                    geom_ap.intersection(geom_fb), n_decimal_places=7
                                )
                            ap_geom_id = insertOrGetGeom(cursor, geom_ap)
                            geom_ap = getMaybeGeom(cursor, geom_id=ap_geom_id)
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
                        else:
                            # if crop in ["alfalfa"] and date_performed is None:
                            #     date_performed = datetime(2021, 6, 1)  # Was probably seeded the previous crop season
                            # elif crop in ["winter wheat"] and date_performed is None:
                            #     date_performed = datetime(2021, 10, 15)
                            # elif crop in ["flowers", "mums", "pumpkins", "sweet corn"] and date_performed is None:
                            #     date_performed = datetime(2022, 6, 1)

                            planting = Act(
                                act_type="PLANT",
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
                    conn.commit()  # Make a commit for every field
    conn.commit()  # Commit outside the with statment

# %%
