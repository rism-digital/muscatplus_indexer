import logging
from typing import List, Tuple, Generator, Dict

from indexer.helpers.db import mysql_pool
from indexer.helpers.solr import submit_to_solr
from indexer.helpers.utilities import parallelise
from indexer.records.person import create_person_index_documents, PersonIndexDocument

log = logging.getLogger("muscat_indexer")


def _get_people_groups(cfg: Dict) -> Generator[Dict, None, None]:
    conn = mysql_pool.connection()
    curs = conn.cursor()

    curs.execute("""SELECT id, marc_source FROM muscat_development.people WHERE wf_stage = 1;""")

    while rows := curs._cursor.fetchmany(cfg['mysql']['resultsize']):  # noqa
        yield rows

    curs.close()
    conn.close()


def index_people(cfg: Dict) -> bool:
    people_groups = _get_people_groups(cfg)
    parallelise(people_groups, index_people_groups)

    return True


def index_people_groups(people: List) -> bool:
    log.info("Indexing People")
    records_to_index: List = []

    for record in people:
        m_source = record['marc_source']
        docs = create_person_index_documents(m_source)
        records_to_index.extend(docs)

    check: bool = submit_to_solr(records_to_index)

    if not check:
        log.error("There was an error submitting to Solr")

    return check
