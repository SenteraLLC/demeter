"""Automatic database diagram generation."""
import argparse
from dotenv import load_dotenv

from sqlalchemy import MetaData
from sqlalchemy_schemadisplay import create_schema_graph

# from analytics_db.models import Base
from demeter.db import getConnection

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("-d", "--show_datatypes", action="store_true")
    parser.add_argument("-i", "--show_indexes", action="store_true")
    parser.add_argument("-o", "--output_path", default="schema-test_demeter.png")
    args = parser.parse_args()

    load_dotenv()
    conn = getConnection(env_name="TEST_DEMETER")
    metadata = MetaData(
        f"postgresql://{conn.info.user}:{conn.info.password}@{conn.info.host}/{conn.info.dbname}",
        schema="test_demeter",
    )

    # restrict_tables = metadata.tables.keys() - ["test_demeter.node_raster"]
    # graph = create_schema_graph(metadata=metadata, show_datatypes=False, show_indexes=False, rankdir="LR", concentrate=False)

    graph = create_schema_graph(
        # metadata=Base.metadata,
        metadata="postgres://user:pwd@host/database",
        show_datatypes=args.show_datatypes,
        show_indexes=args.show_indexes,
        rankdir="LR",
        concentrate=False,
    )
    graph.write_png("schema-test_demeter.png")
