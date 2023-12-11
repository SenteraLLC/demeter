from datetime import datetime

import pytest
from pandas import read_sql_query
from psycopg2.errors import ForeignKeyViolation
from sqlalchemy.sql import text
from sure import expect

from demeter.data import (
    Grouper,
    Organization,
    insertOrGetGrouper,
    insertOrGetOrganization,
)
from demeter.tests.conftest import SCHEMA_NAME

ORGANIZATION = Organization(name="Test Organization")


class TestUpsertGrouper:
    """
    Note: After all the tests in TestUpsertGrouper run, `test_db_class` will clear all data since it has "class" scope.
    """

    def test_insert_get_organization(self, test_db_class):
        with test_db_class.connect() as conn:
            with conn.begin():
                organization_id = insertOrGetOrganization(
                    conn.connection.cursor(), ORGANIZATION
                )

    def test_insert_get_grouper(self, test_db_class):
        with test_db_class.connect() as conn:
            with conn.begin():
                organization_id = insertOrGetOrganization(
                    conn.connection.cursor(), ORGANIZATION
                )
                root_grouper = Grouper(
                    name="Root Grouper",
                    organization_id=organization_id,
                    parent_grouper_id=None,
                )
                root_fg_id = insertOrGetGrouper(conn.connection.cursor(), root_grouper)
                root_fg_id.should.be.equal(1)

                root_fg_id_get = insertOrGetGrouper(
                    conn.connection.cursor(), root_grouper
                )
                root_fg_id_get.should.be.equal(root_fg_id)

    def test_insert_child_grouper(self, test_db_class):
        with test_db_class.connect() as conn:
            with conn.begin():
                organization_id = insertOrGetOrganization(
                    conn.connection.cursor(), ORGANIZATION
                )
                root_grouper = Grouper(
                    name="Root Grouper",
                    organization_id=organization_id,
                    parent_grouper_id=None,
                )
                root_fg_id = insertOrGetGrouper(conn.connection.cursor(), root_grouper)
                root_fg_id.should.be.equal(1)

                child_grouper = Grouper(
                    name="Child Field Group",
                    organization_id=organization_id,
                    parent_grouper_id=root_fg_id,
                )
                child_fg_id = insertOrGetGrouper(
                    conn.connection.cursor(), child_grouper
                )
                child_fg_id.should.be.equal_to(2)

    def test_insert_orphan_grouper(self, test_db_class):
        with test_db_class.connect() as conn:
            with conn.begin():
                organization_id = insertOrGetOrganization(
                    conn.connection.cursor(), ORGANIZATION
                )
                child_grouper = Grouper(
                    name="Child Field Group 2",
                    organization_id=organization_id,
                    parent_grouper_id=10,
                )
                with pytest.raises(ForeignKeyViolation):
                    _ = insertOrGetGrouper(conn.connection.cursor(), child_grouper)

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
