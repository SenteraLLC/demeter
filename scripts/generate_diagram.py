"""Automatic database diagram generation."""
import argparse

from dotenv import load_dotenv
from geoalchemy2 import Geometry  # noqa: F401
from sqlalchemy import MetaData
from sqlalchemy_schemadisplay import create_schema_graph

from demeter.db import getConnection

# Required geoalchemy2.Geometry import for sqlalchemy to use Geometry types

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Generate schema diagram.")
    # parser.add_argument("-d", "--show_datatypes", action="store_true", description="")
    # parser.add_argument("-i", "--show_indexes", action="store_true")
    parser.add_argument(
        "-o",
        "--output_path",
        default="schema-test_demeter.png",
        help="Output path to save diagram.",
    )
    args = parser.parse_args()

    load_dotenv()
    conn = getConnection(env_name="TEST_DEMETER")
    metadata = MetaData(schema="test_demeter")
    metadata.reflect(conn.engine)

    # restrict_tables = metadata.tables.keys() - ["test_demeter.crop_type"]
    graph = create_schema_graph(
        metadata=metadata,
        show_datatypes=False,
        show_indexes=False,
        rankdir="LR",
        concentrate=False,
        # restrict_tables=restrict_tables,
    )
    graph.write_png("dbschema.png")
