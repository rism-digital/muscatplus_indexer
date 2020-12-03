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

    curs.execute("""SELECT p.id AS id, p.marc_source AS marc_source, COUNT(s.source_id) AS source_count
                    FROM muscat_development.people AS p
                    JOIN muscat_development.sources_to_people AS s ON p.id = s.person_id
                    WHERE wf_stage = 1
                    GROUP BY p.id;""")

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
        docs = create_person_index_documents(record)
        records_to_index.extend(docs)

    check: bool = submit_to_solr(records_to_index)

    if not check:
        log.error("There was an error submitting to Solr")

    return check
