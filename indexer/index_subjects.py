import logging

from psycopg.rows import dict_row

from indexer.helpers.db import postgres_pool
from indexer.helpers.solr import submit_to_solr
from indexer.records.subject import SubjectIndexDocument, create_subject_index_document

log = logging.getLogger("muscat_indexer")


def index_subjects(cfg: dict) -> bool:
    log.info("Indexing Subjects")
    record_id = int(cfg["id"]) if "id" in cfg else None
    with postgres_pool.connection() as conn:
        with conn.cursor(row_factory=dict_row) as curs:
            curs.execute(
                """SELECT id, term, alternate_terms, notes
                FROM standard_terms
                WHERE (%s::bigint IS NULL OR id = %s)
                ORDER BY id;""",
                (record_id, record_id),
            )
            all_subjects: list[dict] = curs.fetchall()

    records_to_index: list = []
    for subject in all_subjects:
        doc: SubjectIndexDocument = create_subject_index_document(subject, cfg)
        records_to_index.append(doc)

    check: bool = True if cfg["dry"] else submit_to_solr(records_to_index, cfg)

    if not check:
        log.error("There was an error submitting subjects to Solr")
        return False

    return True
