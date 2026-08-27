import atexit
import logging
from uuid import uuid4

import orjson
from psycopg import Connection
from psycopg.rows import dict_row
from psycopg.types.json import set_json_loads

from indexer.helpers.db import create_postgres_pool

log = logging.getLogger("muscat_indexer")
postgres_pool = create_postgres_pool("diamm")
set_json_loads(orjson.loads)

atexit.register(postgres_pool.close)


def server_side_cursor(conn: Connection, record_type: str):
    """Create a uniquely named, transaction-bound cursor for streamed indexing."""
    return conn.cursor(name=f"diamm_{record_type}_{uuid4().hex}", row_factory=dict_row)
