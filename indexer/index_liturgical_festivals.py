import logging

from psycopg.rows import dict_row

from indexer.helpers.db import postgres_pool
from indexer.helpers.solr import submit_to_solr
from indexer.records.liturgical_festival import (
    LiturgicalFestivalIndexDocument,
    create_liturgical_festival_document,
)

log = logging.getLogger("muscat_indexer")


def index_liturgical_festivals(cfg: dict) -> bool:
    log.info("Indexing Liturgical Festivals")
    record_id = int(cfg["id"]) if "id" in cfg else None
    with postgres_pool.connection() as conn, conn.cursor(row_factory=dict_row) as curs:
        curs.execute(
            """SELECT id, name, alternate_terms, notes
                FROM liturgical_feasts
                WHERE (%s::bigint IS NULL OR id = %s)
                ORDER BY id;""",
            (record_id, record_id),
        )
        all_festivals: list[dict] = curs.fetchall()

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
