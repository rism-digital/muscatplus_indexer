import logging
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

    curs.execute("""SELECT id, marc_source FROM muscat_development.institutions;""")

    while rows := curs._cursor.fetchmany(cfg['mysql']['resultsize']):
        yield rows

    curs.close()
    conn.close()


def index_institutions(cfg: Dict) -> bool:
    institution_groups = _get_institution_groups(cfg)
    parallelise(institution_groups, index_institution_groups)

    return True


def index_institution_groups(institutions: List) -> bool:
    log.debug("Indexing Institutions")
    records_to_index: List = []

    for record in institutions:
        m_source = record['marc_source']
        try:
            doc: InstitutionIndexDocument = create_institution_index_document(m_source)
        except RequiredFieldException:
            log.error("A required field was not found, so this document was not indexed.")
            continue

        records_to_index.append(doc)

    check: bool = submit_to_solr(records_to_index)

    if not check:
        log.error("There was an error submitting institutions to Solr")

    return check
