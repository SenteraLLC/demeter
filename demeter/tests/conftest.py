from os.path import dirname, join, realpath
import pytest

from contextlib import contextmanager
from dotenv import load_dotenv
from sqlalchemy import MetaData
from sqlalchemy.orm import declarative_base, sessionmaker

from demeter.db import getConnection

load_dotenv()

SCHEMA_NAME = "test_demeter"
ROOT_DIR = realpath(join(dirname(__file__), "..", ".."))
DEMETER_DIR = realpath(join(dirname(__file__), ".."))
TEST_DIR = realpath(dirname(__file__))

# engine = create_engine('postgresql://...')
conn = getConnection(env_name="TEST_DEMETER")
engine = conn.engine
Base = declarative_base()


metadata_obj = MetaData(schema="test_demeter")
metadata_obj.reflect(conn.engine)
# metadata_obj.tables["test_demeter.geom"]


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


@pytest.fixture(scope="session")
def test_db_session():
    clear_tables()  # Ensure tables are empty before beginning the tests
    yield engine
    engine.dispose()
    clear_tables()


# @pytest.fixture(autouse=True, scope="session")
# def CONNECTION_FIXTURE():
#     yield getConnection(env_name="TEST_DEMETER")


# @pytest.fixture(autouse=True, scope="session")
# def CURSOR_FIXTURE(CONNECTION_FIXTURE):
#     yield CONNECTION_FIXTURE.connection.cursor()


# def setup_db(conn: Connection):
#     """Loads schema.sql from repo's root directory and runs it in test_demeter schema."""
#     import subprocess
#     from tempfile import NamedTemporaryFile

#     with NamedTemporaryFile() as tmp:
#         with open(join(ROOT_DIR, "schema.sql"), "r") as schema_f:
#             schema_sql = schema_f.read()
#             # Change schema name in SQL script if needed.
#             if SCHEMA_NAME != "test_demeter":
#                 schema_sql = schema_sql.replace("test_demeter", SCHEMA_NAME)
#         tmp.write(schema_sql.encode())  # Writes SQL script to a temp file
#         host = conn.engine.url.host
#         username = conn.engine.url.username
#         password = conn.engine.url.password
#         database = conn.engine.url.database
#         psql = f'PGPASSWORD={password} psql -h {host} -U {username} -f "{tmp.name}" {database}'
#         subprocess.call(psql, shell=True)


# def teardown_db(conn: Connection):
#     stmt = "DROP SCHEMA IF EXISTS test_demeter CASCADE;"  # Don't know why I can't pass schema as a var?
#     conn.execute(stmt)  # Delete test schema
#     # host = conn.engine.url.host
#     # username = conn.engine.url.username
#     # password = conn.engine.url.password
#     # database = conn.engine.url.database
#     # stmt = text("DROP SCHEMA IF EXISTS test_demeter CASCADE;")
#     # psql = f'PGPASSWORD={password} psql -h {host} -U {username} {database} '{}'
#     # subprocess.call(psql, shell=True)
#     # stmt = text("DROP SCHEMA IF EXISTS :schema_name CASCADE;")
#     # conn_public.engine.execute(stmt, {"schema_name": SCHEMA_NAME})  # Delete test schema


# @pytest.fixture(autouse=True, scope="session")
# def SETUP_DB_FIXTURE(CONNECTION_FIXTURE, CONNECTION_FIXTURE_PUBLIC):
#     setup_db(CONNECTION_FIXTURE)
#     yield None
#     teardown_db(CONNECTION_FIXTURE_PUBLIC)
