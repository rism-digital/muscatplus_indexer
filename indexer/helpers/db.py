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

    log.info("Creating tombstone view")
    curs.execute(f"""CREATE OR REPLACE VIEW {dbname}.tombstones AS
                 SELECT v.item_type AS item_type, v.item_id AS item_id,
                        v.created_at AS deleted,
                        (SELECT TRIM(
                            CASE
                                WHEN v.object LIKE '%std_title:%' THEN
                                    SUBSTRING_INDEX(
                                            SUBSTRING_INDEX(v.object, 'std_title:', -1),
                                            '\n',
                                            1
                                    )
                                WHEN v.object LIKE '%source_id:%' THEN
                                    SUBSTRING_INDEX(
                                            CONCAT('sources/', SUBSTRING_INDEX(v.object, 'source_id: ', -1)),
                                            '\n',
                                            1
                                    )
                                WHEN v.object LIKE '%full_name:%' THEN
                                    SUBSTRING_INDEX(
                                            SUBSTRING_INDEX(v.object, 'full_name:', -1),
                                            '\n',
                                            1
                                    )
                                WHEN v.object LIKE '%title:%' THEN
                                    SUBSTRING_INDEX(
                                            SUBSTRING_INDEX(v.object, 'title:', -1),
                                            '\n',
                                            1
                                    )

                                WHEN v.object LIKE '%name:%' THEN
                                    SUBSTRING_INDEX(
                                            SUBSTRING_INDEX(v.object, 'name:', -1),
                                            '\n',
                                            1
                                    )
                                ELSE NULL
                            END)) AS name,
                            (TRIM(
                                BOTH '"' FROM SUBSTRING_INDEX(SUBSTRING_INDEX(v.object, 'marc_source: ', -1),
                                '\n',
                                1))) AS marc_source
                 FROM {dbname}.versions AS v
                 WHERE v.event = 'destroy'
                   AND v.item_type IN ('Source', 'Person', 'Institution', 'Holding')
                 ORDER BY item_type DESC""")  # noqa: S608

    return True
