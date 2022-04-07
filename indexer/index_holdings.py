import logging
from collections import deque
from typing import Generator

from indexer.helpers.db import mysql_pool
from indexer.helpers.solr import submit_to_solr
from indexer.helpers.utilities import parallelise
from indexer.records.holding import create_holding_index_document, HoldingIndexDocument

log = logging.getLogger("muscat_indexer")


def _get_holdings_groups(cfg: dict) -> Generator[dict, None, None]:
    conn = mysql_pool.connection()
    curs = conn.cursor()
    dbname: str = cfg['mysql']['database']

    id_where_clause: str = ""
    if "id" in cfg:
        id_where_clause = f"AND holdings.id = {cfg['id']}"

    # The published / unpublished state is ignored for holding records, so we just take any and all holding records.
    curs.execute(f"""SELECT holdings.id AS id, holdings.source_id AS source_id, holdings.marc_source AS marc_source,
                           sources.std_title AS source_title, sources.composer AS creator_name, 
                           sources.record_type as record_type, sources.marc_source AS source_record_marc
                    FROM {dbname}.holdings AS holdings
                    LEFT JOIN {dbname}.sources AS sources ON holdings.source_id = sources.id
                    WHERE sources.marc_source IS NOT NULL {id_where_clause};""")

    while rows := curs._cursor.fetchmany(cfg['mysql']['resultsize']):  # noqa
        yield rows

    curs.close()
    conn.close()


def index_holdings(cfg: dict) -> bool:
    holdings_groups = _get_holdings_groups(cfg)
    parallelise(holdings_groups, index_holdings_groups, cfg)

    return True


def index_holdings_groups(holdings: list, cfg: dict) -> bool:
    log.info("Indexing Holdings")
    records_to_index: deque = deque()

    for record in holdings:
        doc: HoldingIndexDocument = create_holding_index_document(record, cfg)
        records_to_index.append(doc)

    check: bool = submit_to_solr(list(records_to_index), cfg)

    if not check:
        log.error("There was an error submitting holdings to Solr")

    return check
