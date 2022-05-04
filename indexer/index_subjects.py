import logging

from indexer.helpers.db import mysql_pool
from indexer.helpers.solr import submit_to_solr
from indexer.records.subject import SubjectIndexDocument, create_subject_index_document

log = logging.getLogger("muscat_indexer")


def index_subjects(cfg: dict) -> bool:
    log.info("Indexing Subjects")
    conn = mysql_pool.connection()
    curs = conn.cursor()
    dbname: str = cfg['mysql']['database']

    id_where_clause: str = ""
    if "id" in cfg:
        id_where_clause = f"WHERE id = {cfg['id']}"

    curs.execute(f"""SELECT 
        id, 
        term, 
        alternate_terms, 
        notes
        FROM {dbname}.standard_terms 
        {id_where_clause};""")

    all_subjects: list[dict] = curs._cursor.fetchall()

    records_to_index: list = []
    for subject in all_subjects:
        doc: SubjectIndexDocument = create_subject_index_document(subject, cfg)
        records_to_index.append(doc)

    if cfg["dry"]:
        check = True
    else:
        check = submit_to_solr(records_to_index, cfg)

    if not check:
        log.error("There was an error submitting subjects to Solr")
        return False

    return True
