from contextlib import contextmanager
from os.path import (
    dirname,
    join,
    realpath,
)

import pytest
from dotenv import load_dotenv
from geoalchemy2 import Geometry  # Required import for sqlalchemy to use Geometry types
from psycopg2.extensions import AsIs
from sqlalchemy import MetaData
from sqlalchemy.orm import sessionmaker

from demeter.db import getConnection
from initialize.schema.demeter._initialize import initialize_demeter_instance

load_dotenv()

# prepare database working environment
SCHEMA_NAME = "test_demeter"


@pytest.fixture(scope="function")
def schema_name():
    yield SCHEMA_NAME


ROOT_DIR = realpath(join(dirname(__file__), "..", ".."))
DEMETER_DIR = realpath(join(dirname(__file__), ".."))
TEST_DIR = realpath(dirname(__file__))

c = getConnection(env_name="TEST_DEMETER_SETUP")
engine = c.engine

# check to make sure a test demeter instance doesn't already exist; if so, we need to manually drop that schema
# else create instance for running tests
created_test_demeter_instance = initialize_demeter_instance(
    conn=c, schema_name=SCHEMA_NAME, drop_existing=False
)
msg = "Test schema already exists. Did you create a test schema for dev work? Please check and manually drop it."
assert created_test_demeter_instance, msg

metadata_obj = MetaData(schema=SCHEMA_NAME)
metadata_obj.reflect(c.engine)

c_read_write = getConnection(env_name="TEST_DEMETER_RW")
engine_read_write = c_read_write.engine


@contextmanager
def session_scope(engine, schema=None):
    """Provide a transactional scope around a series of operations.
    From: https://stackoverflow.com/questions/67255653/how-to-set-up-and-tear-down-a-database-between-tests-in-fastapi
    """
    schema = "" if schema is None else schema
    Session = sessionmaker(bind=engine)  # noqa
    session = Session()
    session.execute(
        "SET search_path TO :schema,public", {"schema": schema}
    )  # public needed because that's where PostGIS ext lives

    try:
        yield session
        session.commit()
    except:
        session.rollback()
        raise
    finally:
        session.close()


def clear_tables():
    with session_scope(engine, schema=SCHEMA_NAME) as conn:
        for table in metadata_obj.sorted_tables:
            conn.execute(f"TRUNCATE {table.name} RESTART IDENTITY CASCADE;")
        conn.commit()


@pytest.fixture(scope="class")
def test_db_class():
    """Class scope - do not use this after `test_db_session` has been used."""
    clear_tables()  # Ensure tables are empty before beginning the tests
    yield engine_read_write
    engine_read_write.dispose()
    clear_tables()


@pytest.fixture(scope="function")
def test_read_only_access():
    """Function scope - create connection for read only user testing."""
    c_read_only = getConnection(env_name="TEST_DEMETER_RO")
    engine_read_only = c_read_only.engine
    yield engine_read_only
    c_read_only.close()


@pytest.fixture(scope="session", autouse=True)
def test_db_session_teardown():
    """Session scope - DB schema drop occurs after all tests in the session have been completed."""
    yield None
    with engine.connect() as conn:
        with conn.begin():
            stmt = """DROP SCHEMA IF EXISTS %s CASCADE;"""
            params = AsIs(SCHEMA_NAME)
            conn.execute(stmt, params)
    c_read_write.close()
    c.close()


# @pytest.fixture(autouse=True, scope="session")
# def SETUP_DB_FIXTURE():
#     setup_db(conn)
#     yield None
#     teardown_db(conn)


# @pytest.fixture(scope="session")
# def test_db_session():
#     """Session scope - after using this, do not use any other db connection fixutre."""
#     clear_tables()  # Ensure tables are empty before beginning the tests
#     yield engine
#     engine.dispose()
#     clear_tables()


# @pytest.fixture(autouse=True, scope="session")
# def CONNECTION_FIXTURE():
#     yield getConnection(env_name=ENV_NAME)


# @pytest.fixture(autouse=True, scope="session")
# def CURSOR_FIXTURE(CONNECTION_FIXTURE):
#     yield CONNECTION_FIXTURE.connection.cursor()
