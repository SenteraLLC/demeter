from datetime import datetime

import pytest
from pandas import read_sql_query
from pandas.testing import assert_frame_equal
from psycopg2.errors import InsufficientPrivilege
from psycopg2.extensions import AsIs
from shapely.geometry import Point
from sqlalchemy.exc import ProgrammingError
from sqlalchemy.sql import text
from sure import expect

from demeter.data import (
    FieldGroup,
    getMaybeFieldGroupId,
    insertOrGetFieldGroup,
)
from demeter.tests.conftest import SCHEMA_NAME


class TestUserPrivileges:
    """
    Note: After all the tests in TestUserPrivileges run, `test_db_class` will clear all data since it has "class" scope.
    """

    ## TESTS FOR DEMETER_USER
    def test_demeter_user_can_insert(self, test_db_class):
        with test_db_class.connect() as conn:
            with conn.begin():
                cursor = conn.connection.cursor()
                stmt = """insert into field_group (name)
                values ('Testing User Privileges');
                """
                cursor.execute(stmt)

                stmt = """select name from field_group where name = 'Testing User Privileges'"""
                cursor.execute(stmt)
                len(cursor.fetchall()).should.be.equal(1)

    def test_demeter_user_can_read(self, test_db_class):
        with test_db_class.connect() as conn:
            with conn.begin():
                test_field_group = FieldGroup(
                    name="Testing User Privileges", parent_field_group_id=None
                )
                test_fg_id = getMaybeFieldGroupId(
                    conn.connection.cursor(), test_field_group
                )
                test_fg_id.should.be.equal(1)

    def test_demeter_user_cannot_update(self, test_db_class):
        with test_db_class.connect() as conn:
            with conn.begin():
                stmt = """update field_group
                set name = 'name change'
                where field_group_id = 1"""

                with pytest.raises(ProgrammingError):
                    conn.execute(stmt)

    def test_demeter_user_cannot_delete(self, test_db_class):
        with test_db_class.connect() as conn:
            with conn.begin():
                stmt = """delete from field_group
                where field_group_id = 1"""

                with pytest.raises(ProgrammingError):
                    conn.execute(stmt)

    def test_demeter_user_cannot_drop_schema(self, test_db_class, schema_name):
        with test_db_class.connect() as conn:
            with conn.begin():
                stmt = """DROP SCHEMA IF EXISTS %s CASCADE;"""
                params = AsIs(schema_name)

                with pytest.raises(ProgrammingError):
                    conn.execute(stmt, params)

    def test_demeter_user_cannot_drop_table(self, test_db_class):
        with test_db_class.connect() as conn:
            with conn.begin():
                stmt = """DROP TABLE IF EXISTS field CASCADE;"""
                with pytest.raises(ProgrammingError):
                    conn.execute(stmt)

    ## TEST FOR DEMETER_RO_USER

    def test_demeter_ro_user_can_read(self, test_read_only_access):
        with test_read_only_access.connect() as conn:
            with conn.begin():
                test_field_group = FieldGroup(
                    name="Testing User Privileges", parent_field_group_id=None
                )
                test_fg_id = getMaybeFieldGroupId(
                    conn.connection.cursor(), test_field_group
                )
                test_fg_id.should.be.equal(1)

    def test_demeter_ro_user_cannot_insert(self, test_read_only_access):
        with test_read_only_access.connect() as conn:
            with conn.begin():
                cursor = conn.connection.cursor()
                stmt = """insert into field_group (name)
                values ('Testing User Privileges - 2');
                """

                with pytest.raises(InsufficientPrivilege):
                    cursor.execute(stmt)

    def test_demeter_ro_user_cannot_update(self, test_read_only_access):
        with test_read_only_access.connect() as conn:
            with conn.begin():
                stmt = """update field_group
                set name = 'name change'
                where field_group_id = 1"""

                with pytest.raises(ProgrammingError):
                    conn.execute(stmt)

    def test_demeter_ro_user_cannot_delete(self, test_read_only_access):
        with test_read_only_access.connect() as conn:
            with conn.begin():
                stmt = """delete from field_group
                where field_group_id = 1"""

                with pytest.raises(ProgrammingError):
                    conn.execute(stmt)

    def test_demeter_ro_user_cannot_drop_schema(
        self, test_read_only_access, schema_name
    ):
        with test_read_only_access.connect() as conn:
            with conn.begin():
                stmt = """DROP SCHEMA IF EXISTS %s CASCADE;"""
                params = AsIs(schema_name)

                with pytest.raises(ProgrammingError):
                    conn.execute(stmt, params)

    def test_demeter_ro_user_cannot_drop_table(self, test_read_only_access):
        with test_read_only_access.connect() as conn:
            with conn.begin():
                stmt = """DROP TABLE IF EXISTS field CASCADE;"""
                with pytest.raises(ProgrammingError):
                    conn.execute(stmt)
