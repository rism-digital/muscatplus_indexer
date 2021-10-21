import logging
from collections import deque
from typing import List, Generator, Dict

from indexer.helpers.db import mysql_pool
from indexer.helpers.solr import submit_to_solr
from indexer.helpers.utilities import parallelise
from indexer.records.person import create_person_index_document

log = logging.getLogger("muscat_indexer")


def _get_people_groups(cfg: Dict) -> Generator[Dict, None, None]:
    conn = mysql_pool.connection()
    curs = conn.cursor()
    dbname: str = cfg['mysql']['database']

    curs.execute(f"""SELECT p.id AS id, p.marc_source AS marc_source, COUNT(DISTINCT ps.source_id) AS source_count,
                     p.created_at AS created, p.updated_at AS updated
                     FROM {dbname}.people AS p
                     LEFT JOIN {dbname}.sources_to_people AS ps ON p.id = ps.person_id
                     LEFT JOIN {dbname}.sources AS s ON ps.source_id = s.id
                     WHERE p.wf_stage = 1 AND (s.wf_stage is NULL OR s.wf_stage = 1)
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

    records_to_index: deque = deque()
    for record in people:
        doc = create_person_index_document(record)
        records_to_index.append(doc)

    check: bool = submit_to_solr(list(records_to_index))

    if not check:
        log.error("There was an error submitting to Solr")

    return check
