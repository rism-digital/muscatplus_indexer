import logging
from typing import Dict, List

from indexer.helpers.db import mysql_pool
from indexer.helpers.solr import submit_to_solr
from indexer.records.liturgical_festival import LiturgicalFestivalIndexDocument, create_liturgical_festival_document

log = logging.getLogger("muscat_indexer")


def index_liturgical_festivals(cfg: Dict) -> bool:
    log.info("Indexing liturgical festivals")
    conn = mysql_pool.connection()
    curs = conn.cursor()
    dbname: str = cfg['mysql']['database']

    curs.execute(f"""SELECT
    id,
    name,
    alternate_terms,
    notes
    FROM {dbname}.liturgical_feasts""")

    all_festivals: List[Dict] = curs._cursor.fetchall()

    records_to_index: List = []

    for festival in all_festivals:
        doc: LiturgicalFestivalIndexDocument = create_liturgical_festival_document(festival)
        records_to_index.append(doc)

    check: bool = submit_to_solr(records_to_index)

    if not check:
        log.error("There was an error submitting festivals to Solr")
        return False

    return True
