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

    curs.execute(f"""SELECT p.id AS id, p.marc_source AS marc_source,
                     p.created_at AS created, p.updated_at AS updated,
                    (SELECT COUNT(DISTINCT sp.source_id)
                        FROM {dbname}.sources_to_people AS sp
                        LEFT JOIN {dbname}.sources AS ss ON sp.source_id = ss.id
                        WHERE sp.person_id = p.id AND (ss.wf_stage IS NULL OR ss.wf_stage = 1)) 
                        AS source_count,
                    (SELECT COUNT(DISTINCT hp.holding_id)
                        FROM {dbname}.holdings_to_people AS hp
                        LEFT JOIN {dbname}.holdings AS hh ON hp.holding_id = hh.id
                        WHERE hp.person_id = p.id AND (hh.wf_stage IS NULL OR hh.wf_stage = 1))
                        AS holdings_count
                     FROM {dbname}.people AS p
                     WHERE p.wf_stage = 1;""")

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
