from datetime import datetime

import pytest
from pandas import read_sql_query
from psycopg2.errors import ForeignKeyViolation
from shapely.geometry import Point
from sqlalchemy.sql import text
from sure import expect

from demeter.data import (
    Field,
    FieldGroup,
    getFieldGroupAncestors,
    getFieldGroupDescendants,
    getFieldGroupFields,
    insertOrGetField,
    insertOrGetFieldGroup,
    insertOrGetGeom,
)
from demeter.tests.conftest import SCHEMA_NAME


class TestUpsertFieldGroup:
    """
    Note: After all the tests in TestUpsertFieldGroup run, `test_db_class` will clear all data since it has "class" scope.
    """

    def test_insert_get_field_group(self, test_db_class):
        with test_db_class.connect() as conn:
            with conn.begin():

                root_field_group = FieldGroup(
                    name="Root Field Group", parent_field_group_id=None
                )
                root_fg_id = insertOrGetFieldGroup(
                    conn.connection.cursor(), root_field_group
                )
                root_fg_id.should.be.equal(1)

                root_fg_id_get = insertOrGetFieldGroup(
                    conn.connection.cursor(), root_field_group
                )
                root_fg_id_get.should.be.equal(root_fg_id)

    def test_insert_child_field_group(self, test_db_class):
        with test_db_class.connect() as conn:
            with conn.begin():

                root_field_group = FieldGroup(
                    name="Root Field Group", parent_field_group_id=None
                )
                root_fg_id = insertOrGetFieldGroup(
                    conn.connection.cursor(), root_field_group
                )

                child_field_group = FieldGroup(
                    name="Child Field Group", parent_field_group_id=root_fg_id
                )
                child_fg_id = insertOrGetFieldGroup(
                    conn.connection.cursor(), child_field_group
                )
                child_fg_id.should.be.equal_to(2)

    def test_insert_orphan_field_group(self, test_db_class):
        with test_db_class.connect() as conn:
            with conn.begin():

                child_field_group = FieldGroup(
                    name="Child Field Group 2", parent_field_group_id=10
                )
                with pytest.raises(ForeignKeyViolation):
                    _ = insertOrGetFieldGroup(
                        conn.connection.cursor(), child_field_group
                    )

    def test_get_field_group_ancestors(self, test_db_class):
        with test_db_class.connect() as conn:
            with conn.begin():

                list_length_1 = getFieldGroupAncestors(conn.connection.cursor(), 1)
                len(list_length_1).should.be.equal_to(1)

                list_length_2 = getFieldGroupAncestors(conn.connection.cursor(), 2)
                len(list_length_2).should.be.equal_to(2)

                with pytest.raises(Exception):
                    _ = getFieldGroupAncestors(conn.connection.cursor(), 3)

    def test_get_field_group_descendants(self, test_db_class):
        with test_db_class.connect() as conn:
            with conn.begin():

                list_length_1 = getFieldGroupDescendants(conn.connection.cursor(), 2)
                len(list_length_1).should.be.equal_to(1)

                list_length_2 = getFieldGroupDescendants(conn.connection.cursor(), 1)
                len(list_length_2).should.be.equal_to(2)

                with pytest.raises(Exception):
                    _ = getFieldGroupDescendants(conn.connection.cursor(), 3)

    def test_get_field_group_field_in_child(self, test_db_class):
        with test_db_class.connect() as conn:
            with conn.begin():

                # add field to child
                field_geom = Point(0, 0)
                field_geom_id = insertOrGetGeom(conn.connection.cursor(), field_geom)
                field = Field(
                    geom_id=field_geom_id,
                    date_start=datetime(2022, 1, 1),
                    name="Test Field",
                    field_group_id=2,
                )
                _ = insertOrGetField(conn.connection.cursor(), field)

                with pytest.raises(Exception):
                    _ = getFieldGroupFields(
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
                    field_group_id=1,
                )
                _ = insertOrGetField(conn.connection.cursor(), field_2)

                include_descendants = getFieldGroupFields(
                    conn.connection.cursor(), 1, include_descendants=True
                )
                len(include_descendants).should.be.equal_to(2)

                exclude_descendants = getFieldGroupFields(
                    conn.connection.cursor(), 1, include_descendants=False
                )
                len(exclude_descendants).should.be.equal_to(1)

                include_descendants_child = getFieldGroupFields(
                    conn.connection.cursor(), 2, include_descendants=True
                )
                len(include_descendants_child).should.be.equal_to(1)

                exclude_descendants_child = getFieldGroupFields(
                    conn.connection.cursor(), 2, include_descendants=False
                )
                len(exclude_descendants_child).should.be.equal_to(1)

    def test_read_field_group_table(self, test_db_class):
        with test_db_class.connect() as conn:
            with conn.begin():

                sql = text(
                    """
                    select * from field_group
                    """
                )
                df = read_sql_query(sql, conn, params={"schema_name": SCHEMA_NAME})
                len(df).should.be.greater_than(1)


class TestClearGeomData:
    """Tests whether the data upserted in TestUpsertGeom was cleared."""

    def test_read_field_group_table_nodata(self, test_db_class):
        with test_db_class.connect() as conn:
            with conn.begin():

                sql = text(
                    """
                    select * from geom
                    """
                )
                df = read_sql_query(sql, conn, params={"schema_name": SCHEMA_NAME})

                len(df).should.be.equal_to(0)
