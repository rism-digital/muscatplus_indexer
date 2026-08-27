import atexit
import logging
from uuid import uuid4

import yaml
from psycopg import Connection
from psycopg.rows import dict_row
from psycopg_pool import ConnectionPool

log = logging.getLogger("muscat_indexer")
idx_config: dict = yaml.full_load(open("./index_config.yml"))  # noqa: SIM115

config: dict = {
    "user": idx_config["postgres"]["username"],
    "password": idx_config["postgres"]["password"],
    "db": idx_config["postgres"]["cantus_db"],
    "host": idx_config["postgres"]["server"],
}

server_connection: str
if idx_config["postgres"]["server"]:
    server_connection = f"hostaddr={config['host']}"
else:
    server_connection = ""


postgres_pool = ConnectionPool(
    f"{server_connection} dbname={config['db']} user={config['user']} password={config['password']}",
    min_size=0,
    max_size=1,
)

atexit.register(postgres_pool.close)


def server_side_cursor(conn: Connection, record_type: str):
    """Create a uniquely named, transaction-bound cursor for streamed indexing."""
    return conn.cursor(
        name=f"cantus_{record_type}_{uuid4().hex}", row_factory=dict_row
    )
