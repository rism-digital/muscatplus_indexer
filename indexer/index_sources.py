import logging
from typing import Tuple, Generator, List, Optional, Dict

from indexer.helpers.db import mysql_pool
from indexer.helpers.solr import submit_to_solr, solr_conn
from indexer.helpers.utilities import parallelise
from indexer.records.source import SourceIndexDocument, create_source_index_documents

log = logging.getLogger("muscat_indexer")


def _get_parent_sources(cfg: Dict) -> Generator[Dict, None, None]:
    log.info("Getting list of sources to index")
    conn = mysql_pool.connection()
    curs = conn.cursor()

    curs.execute("""SELECT child.id AS id, child.title AS title, child.source_id AS source_id, 
                           child.marc_source AS marc_source, child.record_type AS record_type, 
                           parent.title AS parent_title, COUNT(h.id) AS holdings_count
                    FROM muscat_development.sources AS child
                    LEFT JOIN muscat_development.sources AS parent ON parent.id = child.source_id
                    LEFT JOIN muscat_development.holdings h on child.id = h.source_id
                    WHERE child.wf_stage = 1
                    GROUP BY child.id
                    ORDER BY child.id desc;""")

    while rows := curs._cursor.fetchmany(cfg['mysql']['resultsize']):  # noqa
        yield rows

    curs.close()
    conn.close()


def index_sources(cfg: Dict) -> bool:
    log.info("Indexing sources")
    source_groups = _get_parent_sources(cfg)
    parallelise(source_groups, index_source_groups)

    return True


def index_source_groups(sources: List) -> bool:
    log.info("Index source group")
    records_to_index: List = []

    for record in sources:
        docs = create_source_index_documents(record)
        log.debug("Appending source document")
        records_to_index.extend(docs)

    check: bool = submit_to_solr(records_to_index)

    if not check:
        log.error("There was an error submitting to Solr!")

    return check
