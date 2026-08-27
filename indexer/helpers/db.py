import atexit
import logging
from pathlib import Path
from typing import Any
from uuid import uuid4

import orjson
import yaml
from psycopg import Connection
from psycopg.conninfo import make_conninfo
from psycopg.rows import dict_row
from psycopg.types.json import set_json_loads
from psycopg_pool import ConnectionPool

log = logging.getLogger("muscat_indexer")


def create_postgres_pool(
    project: str, path: str | Path = "index_config.yml"
) -> ConnectionPool:
    return ConnectionPool(
        postgres_conninfo(project, load_index_config(path)),
        min_size=0,
        max_size=1,
    )


def server_side_cursor(conn: Connection, record_type: str):
    """Create a uniquely named, transaction-bound cursor for streamed indexing."""
    cursor_name = f"muscat_{record_type}_{uuid4().hex}"
    return conn.cursor(name=cursor_name, row_factory=dict_row)


def run_preflight_queries(cfg: dict) -> bool:
    """Retain the indexing hook; PostgreSQL requires no MariaDB collation workaround."""
    log.info("No Muscat database preflight queries are required for PostgreSQL.")
    return True


def load_index_config(path: str | Path = "index_config.yml") -> dict[str, Any]:
    with Path(path).open() as config_file:
        return yaml.full_load(config_file)


def project_connection_info(
    project: str, index_config: dict[str, Any]
) -> dict[str, str]:
    postgres = index_config["postgres"]
    project_config = postgres[project]
    return {
        "server": postgres["server"],
        "username": project_config["username"],
        "password": project_config["password"],
        "database": project_config["database"],
    }


def postgres_conninfo(project: str, index_config: dict[str, Any]) -> str:
    connection = project_connection_info(project, index_config)
    parameters = {
        "dbname": connection["database"],
        "user": connection["username"],
        "password": connection["password"],
    }
    if connection["server"]:
        parameters["hostaddr"] = connection["server"]
    return make_conninfo(**parameters)


postgres_pool = create_postgres_pool("muscat")
set_json_loads(orjson.loads)
atexit.register(postgres_pool.close)
