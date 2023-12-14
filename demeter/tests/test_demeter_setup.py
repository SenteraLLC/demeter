from datetime import datetime

import pytest
from pandas import read_sql_query
from pandas.testing import assert_frame_equal
from psycopg2.errors import InsufficientPrivilege
from psycopg2.extensions import AsIs
from shapely.geometry import Point, Polygon
from sqlalchemy.exc import ProgrammingError
from sqlalchemy.sql import text
from sure import expect

from demeter.data import (
    Grouper,
    Organization,
    getMaybeGrouperId,
    getMaybeOrganizationId,
    insertOrGetGeom,
    insertOrGetGrouper,
    insertOrGetOrganization,
)
from demeter.tests.conftest import SCHEMA_NAME

ORGANIZATION = Organization(name="Test Organization")


class TestUserPrivileges:
    """
    Note: After all the tests in TestUserPrivileges run, `test_db_class` will clear all data since it has "class" scope.
    """

    ## TESTS FOR DEMETER_USER
    def test_demeter_user_can_insert(self, test_db_class):
        with test_db_class.connect() as conn:
            with conn.begin():
                organization_id = insertOrGetOrganization(
                    conn.connection.cursor(), ORGANIZATION
                )

                grouper = Grouper(
                    name="Testing User Privileges",
                    organization_id=organization_id,
                    parent_grouper_id=None,
                )
                _ = insertOrGetGrouper(conn.connection.cursor(), grouper)

                with conn.connection.cursor() as cursor:
                    stmt2 = """select name from grouper where name = 'Testing User Privileges'"""
                    cursor.execute(stmt2)
                    len(cursor.fetchall()).should.be.equal(1)

    def test_demeter_user_can_read(self, test_db_class):
        with test_db_class.connect() as conn:
            with conn.begin():
                organization_id = insertOrGetOrganization(
                    conn.connection.cursor(), ORGANIZATION
                )
                test_grouper = Grouper(
                    name="Testing User Privileges",
                    organization_id=organization_id,
                    parent_grouper_id=None,
                )
                test_fg_id = getMaybeGrouperId(conn.connection.cursor(), test_grouper)
                test_fg_id.should.be.equal(1)

    def test_demeter_user_cannot_drop_schema(self, test_db_class, schema_name):
        with test_db_class.connect() as conn:
            with conn.begin():
                stmt = """DROP SCHEMA IF EXISTS :schema CASCADE;"""
                params = {"schema": AsIs(schema_name)}

                with pytest.raises(ProgrammingError):
                    conn.execute(text(stmt), params)

    def test_demeter_user_cannot_drop_table(self, test_db_class):
        with test_db_class.connect() as conn:
            with conn.begin():
                stmt = """DROP TABLE IF EXISTS field CASCADE;"""
                with pytest.raises(ProgrammingError):
                    conn.execute(text(stmt))

    ## TEST FOR DEMETER_RO_USER

    def test_demeter_ro_user_can_read(self, test_read_only_access):
        with test_read_only_access.connect() as conn:
            with conn.begin():
                organization_id = getMaybeOrganizationId(
                    conn.connection.cursor(), ORGANIZATION
                )
                test_grouper = Grouper(
                    name="Testing User Privileges",
                    organization_id=organization_id,
                    parent_grouper_id=None,
                )
                test_fg_id = getMaybeGrouperId(conn.connection.cursor(), test_grouper)
                test_fg_id.should.be.equal(1)

    def test_demeter_ro_user_cannot_insert(self, test_read_only_access):
        with test_read_only_access.connect() as conn:
            with conn.begin():
                cursor = conn.connection.cursor()
                stmt = """insert into grouper (name)
                values ('Testing User Privileges - 2');
                """

                with pytest.raises(InsufficientPrivilege):
                    cursor.execute(stmt)

    def test_demeter_ro_user_cannot_update(self, test_read_only_access):
        with test_read_only_access.connect() as conn:
            with conn.begin():
                stmt = """update grouper
                set name = 'name change'
                where grouper_id = 1"""

                with pytest.raises(ProgrammingError):
                    conn.execute(text(stmt))

    def test_demeter_ro_user_cannot_delete(self, test_read_only_access):
        with test_read_only_access.connect() as conn:
            with conn.begin():
                stmt = """delete from grouper
                where grouper_id = 1"""

                with pytest.raises(ProgrammingError):
                    conn.execute(text(stmt))

    def test_demeter_ro_user_cannot_drop_schema(
        self, test_read_only_access, schema_name
    ):
        with test_read_only_access.connect() as conn:
            with conn.begin():
                stmt = """DROP SCHEMA IF EXISTS :schema CASCADE;"""
                params = {"schema": AsIs(schema_name)}

                with pytest.raises(ProgrammingError):
                    conn.execute(text(stmt), params)

    def test_demeter_ro_user_cannot_drop_table(self, test_read_only_access):
        with test_read_only_access.connect() as conn:
            with conn.begin():
                stmt = """DROP TABLE IF EXISTS field CASCADE;"""
                with pytest.raises(ProgrammingError):
                    conn.execute(text(stmt))

    ## TEST FOR DEMETER_USER to UPDATE and DELETE

    def test_demeter_user_can_update(self, test_db_class):
        with test_db_class.connect() as conn:
            with conn.begin():
                stmt = """update grouper
                set name = 'name change'
                where grouper_id = 1"""

                with conn.connection.cursor() as cursor:
                    cursor.execute(stmt)
                    cursor.rowcount.should.be.ok

    def test_demeter_user_can_delete(self, test_db_class):
        with test_db_class.connect() as conn:
            with conn.begin():
                stmt = """delete from grouper
                where grouper_id = 1"""

                with conn.connection.cursor() as cursor:
                    cursor.execute(stmt)
                    cursor.rowcount.should.be.ok


# TODO: Test unique constraints
# class TestUniqueConstraints:
#     """
#     Note: After all the tests in TestUniqueConstraints run, `test_db_class` will clear all data since it has "class" scope.
#     """

#     ## TESTS FOR DEMETER_USER
#     def test_field_unique_constraint(self, test_db_class):
#         """UNIQUE NULLS NOT DISTINCT (name, date_start, date_end, geom_id, grouper_id)"""
#         with test_db_class.connect() as conn:
#             with conn.begin():
#                 cursor = conn.connection.cursor()

#                 polygon = Polygon(
#                     [
#                         Point(-93.204429, 44.963191),
#                         Point(-93.202980, 44.963212),
#                         Point(-93.202970, 44.961986),
#                         Point(-93.204411, 44.961998),
#                         Point(-93.204429, 44.963191),
#                     ]
#                 )
#                 geom_id = insertOrGetGeom(conn.connection.cursor(), polygon)
#                 geom_id.should.be.greater_than(-1)

#                 stmt = """INSERT INTO field (name, date_start, date_end, geom_id, grouper_id)
#                 VALUES ('Test Field Unique Constraint', '2000-01-01', '2000-12-31', 1, NULL)
#                 """
#                 cursor.execute(stmt)

#                 stmt = """select name from grouper where name = 'Testing User Privileges'"""
#                 cursor.execute(stmt)
#                 len(cursor.fetchall()).should.be.equal(1)
