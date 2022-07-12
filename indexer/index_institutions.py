import logging
from collections import deque
from typing import Generator

from indexer.exceptions import RequiredFieldException
from indexer.helpers.db import mysql_pool
from indexer.helpers.solr import submit_to_solr
from indexer.helpers.utilities import parallelise
from indexer.records.institution import InstitutionIndexDocument, create_institution_index_document

log = logging.getLogger("muscat_indexer")


def _get_institution_groups(cfg: dict) -> Generator[tuple, None, None]:
    conn = mysql_pool.connection()
    curs = conn.cursor()
    dbname: str = cfg['mysql']['database']

    id_where_clause: str = ""
    if "id" in cfg:
        id_where_clause = f"AND i.id = {cfg['id']}"

    curs.execute(f"""SELECT i.id, i.marc_source, i.siglum,
                        i.created_at AS created, i.updated_at AS updated,
                    (SELECT COUNT(DISTINCT allids)
                        FROM (
                            SELECT DISTINCT ss.id AS allids
                                FROM {dbname}.sources_to_institutions AS si
                                LEFT JOIN {dbname}.sources AS ss on si.source_id = ss.id
                                WHERE si.institution_id = i.id AND (ss.wf_stage IS NULL OR ss.wf_stage = 1)
                            UNION SELECT DISTINCT hi.source_id AS allids
                                  FROM {dbname}.holdings AS hi
                                  LEFT JOIN {dbname}.sources AS hs ON hi.source_id = hs.id
                                  WHERE hi.lib_siglum = i.siglum AND (hs.wf_stage IS NULL OR hs.wf_stage = 1)
                            UNION SELECT DISTINCT hs.id AS allids
                                FROM {dbname}.sources AS hs
                                LEFT JOIN {dbname}.holdings AS hd ON hs.source_id = hd.source_id
                                WHERE hd.lib_siglum = i.siglum AND (hs.wf_stage IS NULL OR hs.wf_stage = 1)
                        ) AS derived) AS total_source_count,
                    (SELECT COUNT(DISTINCT si.source_id)
                       FROM {dbname}.sources_to_institutions AS si
                       LEFT JOIN {dbname}.sources AS ss ON si.source_id = ss.id
                       WHERE si.institution_id = i.id AND (ss.wf_stage IS NULL OR ss.wf_stage = 1))
                       AS source_count,
                    (SELECT COUNT(DISTINCT hi.holding_id)
                        FROM {dbname}.holdings_to_institutions AS hi
                        LEFT JOIN {dbname}.holdings AS hh ON hi.holding_id = hh.id
                        WHERE hi.institution_id = i.id)
                        AS holdings_count,
                    (SELECT GROUP_CONCAT(DISTINCT CONCAT_WS('|', reli.id, reli.siglum, reli.name) SEPARATOR '\n')
                        FROM {dbname}.institutions_to_institutions AS rela
                        LEFT JOIN {dbname}.institutions AS reli ON  reli.id = rela.institution_b_id
                        WHERE rela.institution_a_id = i.id)
                        AS related_institutions
                    FROM {dbname}.institutions AS i
                    WHERE i.siglum IS NOT NULL OR
                        ((SELECT COUNT(hi.holding_id) FROM {dbname}.holdings_to_institutions AS hi WHERE hi.institution_id = i.id) > 0 OR
                         (SELECT COUNT(ii.institution_b_id) FROM {dbname}.institutions_to_institutions AS ii WHERE ii.institution_a_id = i.id) > 0 OR
                         (SELECT COUNT(pi.person_id) FROM {dbname}.people_to_institutions AS pi WHERE pi.institution_id = i.id) > 0 OR
                         (SELECT COUNT(bi.publication_id) FROM {dbname}.publications_to_institutions AS bi WHERE bi.institution_id = i.id) > 0 OR
                         (SELECT COUNT(si.source_id) FROM {dbname}.sources_to_institutions AS si WHERE si.institution_id = i.id) > 0
                        ) {id_where_clause};""")

    while rows := curs._cursor.fetchmany(cfg['mysql']['resultsize']):
        yield rows

    curs.close()
    conn.close()


def index_institutions(cfg: dict) -> bool:
    institution_groups = _get_institution_groups(cfg)
    parallelise(institution_groups, index_institution_groups, cfg)

    return True


def index_institution_groups(institutions: list, cfg: dict) -> bool:
    log.info("Indexing Institutions")
    records_to_index: deque = deque()

    for record in institutions:
        try:
            doc: InstitutionIndexDocument = create_institution_index_document(record, cfg)
        except RequiredFieldException:
            log.error("A required field was not found, so this document was not indexed.")
            continue

        records_to_index.append(doc)

    if cfg["dry"]:
        check = True
    else:
        check = submit_to_solr(list(records_to_index), cfg)

    if not check:
        log.error("There was an error submitting institutions to Solr")

    return check
