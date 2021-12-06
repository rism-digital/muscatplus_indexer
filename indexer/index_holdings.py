import logging
from collections import deque
from typing import Dict, Generator, List

from indexer.helpers.db import mysql_pool
from indexer.helpers.solr import submit_to_solr
from indexer.helpers.utilities import parallelise
from indexer.records.holding import create_holding_index_document, HoldingIndexDocument

log = logging.getLogger("muscat_indexer")


def _get_holdings_groups(cfg: Dict) -> Generator[Dict, None, None]:
    conn = mysql_pool.connection()
    curs = conn.cursor()
    dbname: str = cfg['mysql']['database']

    # The published / unpublished state is ignored for holding records, so we just take any and all holding records.
    curs.execute(f"""SELECT holdings.id AS id, holdings.source_id AS source_id, holdings.marc_source AS marc_source,
                           sources.std_title AS source_title, sources.composer AS creator_name, 
                           sources.record_type as record_type
                    FROM {dbname}.holdings AS holdings
                    LEFT JOIN {dbname}.sources AS sources ON holdings.source_id = sources.id;""")

    while rows := curs._cursor.fetchmany(cfg['mysql']['resultsize']):  # noqa
        yield rows

    curs.close()
    conn.close()


def index_holdings(cfg: Dict) -> bool:
    holdings_groups = _get_holdings_groups(cfg)
    parallelise(holdings_groups, index_holdings_groups)

    return True


def index_holdings_groups(holdings: List) -> bool:
    log.info("Indexing holdings")
    records_to_index: deque = deque()

    for record in holdings:
        doc: HoldingIndexDocument = create_holding_index_document(record)
        records_to_index.append(doc)

    check: bool = submit_to_solr(list(records_to_index))

    if not check:
        log.error("There was an error submitting holdings to Solr")

    return check
