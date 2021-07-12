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

    curs.execute(f"""SELECT i.id, i.marc_source, COUNT(s.id) AS source_count,
                     i.created_at AS created, i.updated_at AS updated 
                     FROM {dbname}.institutions AS i
                     LEFT JOIN {dbname}.sources_to_institutions AS ins ON i.id = ins.institution_id
                     LEFT JOIN {dbname}.sources AS s ON ins.source_id = s.id
                     WHERE i.wf_stage = 1 AND (s.wf_stage IS NULL OR s.wf_stage = 1) 
                     GROUP BY i.id;""")

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
