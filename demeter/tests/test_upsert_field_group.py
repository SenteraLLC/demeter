from datetime import datetime

import pytest
from pandas import read_sql_query
from pandas.testing import assert_frame_equal
from psycopg2.errors import ForeignKeyViolation
from shapely.geometry import Point
from sqlalchemy.sql import text
from sure import expect

from demeter.data import (
    Field,
    Grouper,
    getGrouperAncestors,
    getGrouperDescendants,
    getGrouperFields,
    insertOrGetField,
    insertOrGetGeom,
    insertOrGetGrouper,
)
from demeter.tests.conftest import SCHEMA_NAME


class TestUpsertGrouper:
    """
    Note: After all the tests in TestUpsertGrouper run, `test_db_class` will clear all data since it has "class" scope.
    """

    def test_insert_get_grouper(self, test_db_class):
        with test_db_class.connect() as conn:
            with conn.begin():
                root_grouper = Grouper(name="Root Field Group", parent_group_id=None)
                root_fg_id = insertOrGetGrouper(conn.connection.cursor(), root_grouper)
                root_fg_id.should.be.equal(1)

                root_fg_id_get = insertOrGetGrouper(
                    conn.connection.cursor(), root_grouper
                )
                root_fg_id_get.should.be.equal(root_fg_id)

    def test_insert_child_grouper(self, test_db_class):
        with test_db_class.connect() as conn:
            with conn.begin():
                root_grouper = Grouper(name="Root Field Group", parent_group_id=None)
                root_fg_id = insertOrGetGrouper(conn.connection.cursor(), root_grouper)
                root_fg_id.should.be.equal(1)

                child_grouper = Grouper(
                    name="Child Field Group", parent_group_id=root_fg_id
                )
                child_fg_id = insertOrGetGrouper(
                    conn.connection.cursor(), child_grouper
                )
                child_fg_id.should.be.equal_to(2)

    def test_insert_orphan_grouper(self, test_db_class):
        with test_db_class.connect() as conn:
            with conn.begin():
                child_grouper = Grouper(name="Child Field Group 2", parent_group_id=10)
                with pytest.raises(ForeignKeyViolation):
                    _ = insertOrGetGrouper(conn.connection.cursor(), child_grouper)

    def test_get_grouper_ancestors(self, test_db_class):
        with test_db_class.connect() as conn:
            with conn.begin():
                list_length_1 = getGrouperAncestors(conn.connection.cursor(), 1)
                len(list_length_1).should.be.equal_to(1)

                list_length_2 = getGrouperAncestors(conn.connection.cursor(), 2)
                len(list_length_2).should.be.equal_to(2)

                with pytest.raises(Exception):
                    _ = getGrouperAncestors(conn.connection.cursor(), 3)

                cols = ["distance", "group_id"]
                assert_frame_equal(
                    left=getGrouperAncestors(conn.connection.cursor(), 2)[
                        cols
                    ],  # SQL should have sorted by distance already
                    right=getGrouperAncestors(conn.connection.cursor(), 2)[
                        cols
                    ].sort_values(by="distance"),
                    check_dtype=False,
                )

    def test_get_grouper_descendants(self, test_db_class):
        with test_db_class.connect() as conn:
            with conn.begin():
                list_length_1 = getGrouperDescendants(conn.connection.cursor(), 2)
                len(list_length_1).should.be.equal_to(1)

                list_length_2 = getGrouperDescendants(conn.connection.cursor(), 1)
                len(list_length_2).should.be.equal_to(2)

                with pytest.raises(Exception):
                    _ = getGrouperDescendants(conn.connection.cursor(), 3)

                cols = ["distance", "group_id"]
                assert_frame_equal(
                    left=getGrouperDescendants(conn.connection.cursor(), 2)[cols],
                    right=getGrouperDescendants(conn.connection.cursor(), 2)[
                        cols
                    ].sort_values(by="distance"),
                    check_dtype=False,
                )

    def test_get_grouper_field_in_child(self, test_db_class):
        with test_db_class.connect() as conn:
            with conn.begin():
                # add field to child
                field_geom = Point(0, 0)
                field_geom_id = insertOrGetGeom(conn.connection.cursor(), field_geom)
                field = Field(
                    geom_id=field_geom_id,
                    date_start=datetime(2022, 1, 1),
                    name="Test Field",
                    group_id=2,
                )
                _ = insertOrGetField(conn.connection.cursor(), field)

                with pytest.raises(Exception):
                    _ = getGrouperFields(
                        conn.connection.cursor(), 1, include_descendants=False
                    )

                # add field to root
                field_geom_2 = Point(0, 1)
                field_geom_id_2 = insertOrGetGeom(
                    conn.connection.cursor(), field_geom_2
                )
                field_2 = Field(
                    geom_id=field_geom_id_2,
                    date_start=datetime(2022, 1, 1),
                    name="Test Field 2",
                    group_id=1,
                )
                _ = insertOrGetField(conn.connection.cursor(), field_2)

                include_descendants = getGrouperFields(
                    conn.connection.cursor(), 1, include_descendants=True
                )
                len(include_descendants).should.be.equal_to(2)

                exclude_descendants = getGrouperFields(
                    conn.connection.cursor(), 1, include_descendants=False
                )
                len(exclude_descendants).should.be.equal_to(1)

                include_descendants_child = getGrouperFields(
                    conn.connection.cursor(), 2, include_descendants=True
                )
                len(include_descendants_child).should.be.equal_to(1)

                exclude_descendants_child = getGrouperFields(
                    conn.connection.cursor(), 2, include_descendants=False
                )
                len(exclude_descendants_child).should.be.equal_to(1)

    def test_read_grouper_table(self, test_db_class):
        with test_db_class.connect() as conn:
            with conn.begin():
                sql = text(
                    """
                    select * from grouper
                    """
                )
                df = read_sql_query(sql, conn, params={"schema_name": SCHEMA_NAME})
                len(df).should.be.greater_than(1)


class TestClearGeomData:
    """Tests whether the data upserted in TestUpsertGeom was cleared."""

    def test_read_grouper_table_nodata(self, test_db_class):
        with test_db_class.connect() as conn:
            with conn.begin():
                sql = text(
                    """
                    select * from geom
                    """
                )
                df = read_sql_query(sql, conn, params={"schema_name": SCHEMA_NAME})

                len(df).should.be.equal_to(0)
