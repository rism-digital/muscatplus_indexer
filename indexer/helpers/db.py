import logging

import MySQLdb
import yaml
from dbutils.pooled_db import PooledDB
from MySQLdb.cursors import SSDictCursor

log = logging.getLogger("muscat_indexer")
idx_config: dict = yaml.full_load(open("index_config.yml"))  # noqa: SIM115

config: dict = {
    "user": idx_config["mysql"]["username"],
    "password": idx_config["mysql"]["password"],
    "db": idx_config["mysql"]["database"],
    "host": idx_config["mysql"]["server"],
}


mysql_connection = MySQLdb.connect(**config, cursorclass=SSDictCursor)

mysql_pool = PooledDB(
    **config,
    creator=MySQLdb,
    cursorclass=SSDictCursor,
    maxconnections=6,
    charset="utf8mb4",
    use_unicode=True,
)


def run_preflight_queries(cfg: dict) -> bool:
    """Run queries on the database before doing the indexing. Helps work around some issues
    that sometimes pop up with Muscat.
    """
    log.info("Running preflight queries.")
    conn = mysql_pool.connection()
    curs = conn.cursor()
    dbname: str = cfg["mysql"]["database"]

    # work around a bug with collations.
    curs.execute(
        f"""alter table {dbname}.holdings
            modify lib_siglum varchar(32) collate utf8mb4_0900_as_cs null;
            alter table {dbname}.sources
            modify lib_siglum varchar(32) collate utf8mb4_0900_as_cs null;"""
    )

    return True
