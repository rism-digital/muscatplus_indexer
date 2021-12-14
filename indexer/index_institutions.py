import logging
from collections import deque
from typing import Dict, Tuple, Generator, List

from indexer.exceptions import RequiredFieldException
from indexer.helpers.db import mysql_pool
from indexer.helpers.solr import submit_to_solr
from indexer.helpers.utilities import parallelise
from indexer.records.institution import InstitutionIndexDocument, create_institution_index_document

log = logging.getLogger("muscat_indexer")


def _get_institution_groups(cfg: Dict) -> Generator[Tuple, None, None]:
    conn = mysql_pool.connection()
    curs = conn.cursor()
    dbname: str = cfg['mysql']['database']

    curs.execute(f"""SELECT i.id, i.marc_source,
                        i.created_at AS created, i.updated_at AS updated,
                    (SELECT COUNT(DISTINCT si.source_id)
                       FROM {dbname}.sources_to_institutions AS si
                       LEFT JOIN {dbname}.sources AS ss ON si.source_id = ss.id
                       WHERE si.institution_id = i.id AND (ss.wf_stage IS NULL OR ss.wf_stage = 1))
                       AS source_count,
                    (SELECT COUNT(DISTINCT hi.holding_id)
                        FROM {dbname}.holdings_to_institutions AS hi
                        LEFT JOIN {dbname}.holdings AS hh on hi.holding_id = hh.id
                        WHERE hi.institution_id = i.id AND (hh.wf_stage IS NULL OR hh.wf_stage = 1))
                        AS holdings_count
                    FROM {dbname}.institutions AS i
                    WHERE i.wf_stage = 1;""")

    while rows := curs._cursor.fetchmany(cfg['mysql']['resultsize']):
        yield rows

    curs.close()
    conn.close()


def index_institutions(cfg: Dict) -> bool:
    institution_groups = _get_institution_groups(cfg)
    parallelise(institution_groups, index_institution_groups)

    return True


def index_institution_groups(institutions: List) -> bool:
    log.info("Indexing Institutions")
    records_to_index: deque = deque()

    for record in institutions:
        try:
            doc: InstitutionIndexDocument = create_institution_index_document(record)
        except RequiredFieldException:
            log.error("A required field was not found, so this document was not indexed.")
            continue

        records_to_index.append(doc)

    check: bool = submit_to_solr(list(records_to_index))

    if not check:
        log.error("There was an error submitting institutions to Solr")

    return check
