import pytest
from sure import expect

from geopandas import GeoDataFrame
from pandas import read_sql_query
from shapely.geometry import Point, Polygon
from sqlalchemy.sql import text

from demeter.data import insertOrGetGeom
from demeter.tests.conftest import SCHEMA_NAME
from demeter.tests.constants import TABLES_LIST

# from demeter.db import getConnection
# conn = getConnection(env_name="TEST_DEMETER")


def test_reconcile_tables_list(test_db_session):
    with test_db_session.connect() as conn:
        with conn.begin():
            # conn = test_db_session.connect()
            sql = text(
                """
                SELECT *
                FROM pg_catalog.pg_tables
                WHERE schemaname = :schema_name;
                """
            )
            df = read_sql_query(sql, conn, params={"schema_name": SCHEMA_NAME})
            # df = read_sql_query(sql, c, params={"schema_name": "dem_test"})
            # print(len(df))
            db_tables_list = list(df["tablename"].unique())
            for (
                t1
            ) in (
                db_tables_list
            ):  # check if all created tables are supposed to be created
                t1.should.be.within(TABLES_LIST)
            for t2 in TABLES_LIST:  # check if all expected tables are actually created
                t2.should.be.within(db_tables_list)
            # (set(db_tables_list) == set(TABLES_LIST)).should.be.true


def test_upsert_point(test_db_session):
    with test_db_session.connect() as conn:
        with conn.begin():
            # conn = test_db_session.connect()
            point = Point(-93.203209, 44.962470)
            point_geom_id = insertOrGetGeom(conn.connection.cursor(), point)
            point_geom_id.should.be.greater_than(-1)


def test_upsert_polygon(test_db_session):
    with test_db_session.connect() as conn:
        with conn.begin():
            polygon = Polygon(
                [
                    Point(-93.204429, 44.963191),
                    Point(-93.202980, 44.963212),
                    Point(-93.202970, 44.961986),
                    Point(-93.204411, 44.961998),
                    Point(-93.204429, 44.963191),
                ]
            )
            polygon_geom_id = insertOrGetGeom(conn.connection.cursor(), polygon)
            polygon_geom_id.should.be.greater_than(-1)


def test_read_geom_table(test_db_session):
    with test_db_session.connect() as conn:
        with conn.begin():
            # conn = test_db_session.connect()
            sql = text(
                """
                select * from geom
                """
            )
            gdf = GeoDataFrame.from_postgis(sql, conn, geom_col="geom")
            # print(gdf)
            len(gdf).should.be.greater_than(1)
            # df = read_sql_query(sql, conn, params={"schema_name": SCHEMA_NAME})
