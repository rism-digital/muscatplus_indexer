import logging
from typing import Dict, Generator, List

from indexer.helpers.db import mysql_pool
from indexer.helpers.solr import submit_to_solr
from indexer.helpers.utilities import parallelise
from indexer.records.holding import create_holding_index_document, HoldingIndexDocument

log = logging.getLogger("muscat_indexer")


def _get_holdings_groups(cfg: Dict) -> Generator[Dict, None, None]:
    conn = mysql_pool.connection()
    curs = conn.cursor()

    curs.execute("""SELECT id, source_id, marc_source FROM muscat_development.holdings;""")

    while rows := curs._cursor.fetchmany(cfg['mysql']['resultsize']):  # noqa
        yield rows

    curs.close()
    conn.close()


def index_holdings(cfg: Dict) -> bool:
    holdings_groups = _get_holdings_groups(cfg)
    parallelise(holdings_groups, index_holdings_groups)

    return True


def index_holdings_groups(holdings: List) -> bool:
    log.debug("Indexing holidngs")
    records_to_index: List = []

    for record in holdings:
        m_source: str = record['marc_source']
        m_id: int = record['id']
        m_membership: int = record['source_id']
        doc: HoldingIndexDocument = create_holding_index_document(m_source, m_id, m_membership)
        records_to_index.append(doc)

    check: bool = submit_to_solr(records_to_index)

    if not check:
        log.error("There was an error submitting holdings to Solr")

    return check