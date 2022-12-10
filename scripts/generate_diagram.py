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
    parser.add_argument("-o", "--output_path", default="schema-dem_test.png")
    args = parser.parse_args()

    load_dotenv()
    c = getConnection(
        host_key="DEMETER_HOST_WSL",
        port_key="DEMETER_PORT_WSL",
        pw_key="DEMETER_PASSWORD_WSL",
        user_key="DEMETER_USER_WSL",
        options_key="DEMETER_OPTIONS_WSL",
        db_key="DEMETER_DATABASE_WSL",
    )
    metadata = MetaData(
        f"postgresql://{c.info.user}:{c.info.password}@{c.info.host}/{c.info.dbname}",
        schema="dem_test",
    )

    # restrict_tables = metadata.tables.keys() - ["dem_test.node_raster"]
    # graph = create_schema_graph(metadata=metadata, show_datatypes=False, show_indexes=False, rankdir="LR", concentrate=False)

    graph = create_schema_graph(
        # metadata=Base.metadata,
        metadata="postgres://user:pwd@host/database",
        show_datatypes=args.show_datatypes,
        show_indexes=args.show_indexes,
        rankdir="LR",
        concentrate=False,
    )
    graph.write_png("schema-dem_test.png")
