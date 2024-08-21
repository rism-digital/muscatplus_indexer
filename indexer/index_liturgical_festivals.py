import logging

from indexer.helpers.db import mysql_pool
from indexer.helpers.solr import submit_to_solr
from indexer.records.liturgical_festival import (
    LiturgicalFestivalIndexDocument,
    create_liturgical_festival_document,
)

log = logging.getLogger("muscat_indexer")


def index_liturgical_festivals(cfg: dict) -> bool:
    log.info("Indexing Liturgical Festivals")
    conn = mysql_pool.connection()
    curs = conn.cursor()
    dbname: str = cfg["mysql"]["database"]

    id_where_clause: str = ""
    if "id" in cfg:
        id_where_clause = f"WHERE id = {cfg['id']}"

    curs.execute(
        f"""SELECT
    id,
    name,
    alternate_terms,
    notes
    FROM {dbname}.liturgical_feasts
    {id_where_clause};"""
    )

    all_festivals: list[dict] = curs._cursor.fetchall()

    records_to_index: list = []

    for festival in all_festivals:
        doc: LiturgicalFestivalIndexDocument = create_liturgical_festival_document(
            festival, cfg
        )
        records_to_index.append(doc)

    check = True if cfg["dry"] else submit_to_solr(records_to_index, cfg)

    if not check:
        log.error("There was an error submitting festivals to Solr")
        return False

    return True
