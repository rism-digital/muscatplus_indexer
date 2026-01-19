import logging
from collections import deque
from collections.abc import Generator

from indexer.exceptions import RequiredFieldException
from indexer.helpers.db import mysql_pool
from indexer.helpers.solr import submit_to_solr
from indexer.helpers.utilities import parallelise
from indexer.records.institution import (
    create_institution_index_document,
)

log = logging.getLogger("muscat_indexer")


def _get_institution_groups(cfg: dict) -> Generator[dict]:
    conn = mysql_pool.connection()
    curs = conn.cursor()
    dbname: str = cfg["mysql"]["database"]

    id_where_clause: str = ""
    if "id" in cfg:
        id_where_clause = f"AND i.id = {cfg['id']}"

    sql: str = f"""
SELECT i.id, i.marc_source, i.siglum,
    i.created_at AS created, i.updated_at AS updated,
    (SELECT JSON_ARRAYAGG(DISTINCT si.source_id)
        FROM {dbname}.sources_to_institutions AS si
        LEFT JOIN {dbname}.sources AS ss ON si.source_id = ss.id
        WHERE si.institution_id = i.id
            AND si.marc_tag = '852'
            AND ss.wf_stage = 1
    ) AS source_count,
    (SELECT JSON_ARRAYAGG(DISTINCT hh.source_id)
        FROM {dbname}.holdings_to_institutions AS hi
        LEFT JOIN {dbname}.holdings AS hh ON hi.holding_id = hh.id
        LEFT JOIN {dbname}.sources AS ss ON hh.source_id = ss.id
        WHERE hi.institution_id = i.id
            AND hi.marc_tag = '852'
            AND ss.wf_stage = 1
    ) AS holdings_count,
    (SELECT JSON_ARRAYAGG(DISTINCT si.source_id)
        FROM {dbname}.sources_to_institutions AS si
        LEFT JOIN {dbname}.sources AS ss ON si.source_id = ss.id
        WHERE si.institution_id = i.id
            AND si.marc_tag = '710'
            AND ss.wf_stage = 1
    ) AS other_count,
    (SELECT JSON_ARRAYAGG(DISTINCT hh.source_id)
        FROM {dbname}.holdings_to_institutions AS hi
        LEFT JOIN {dbname}.holdings AS hh ON hi.holding_id = hh.id
        LEFT JOIN {dbname}.sources AS ss ON hh.source_id = ss.id
        WHERE hi.institution_id = i.id
            AND hi.marc_tag = '710'
            AND ss.wf_stage = 1
    ) AS other_holdings_count,
    (SELECT COUNT(DISTINCT pc.id)
        FROM {dbname}.people_to_institutions AS pc
        WHERE pc.institution_id = i.id
            AND pc.marc_tag = '910'
    ) AS people_contribution_count,
    (SELECT JSON_ARRAYAGG(DISTINCT
                 JSON_OBJECT('id', pub.id,
                             'author', pub.author,
                             'title', pub.title,
                             'journal', pub.journal,
                             'date', pub.date,
                             'place', pub.place,
                             'short_name', pub.short_name,
                             'marc_source', pub.marc_source))
        FROM {dbname}.institutions_to_publications ipt2
        LEFT JOIN {dbname}.publications pub ON ipt2.publication_id = pub.id
        WHERE ipt2.institution_id = i.id
    ) AS publication_entries,
    (SELECT JSON_ARRAYAGG(DISTINCT
                             JSON_OBJECT('a_id', CONCAT('institution_', reli.id),
                                         'b_id', CONCAT('institution_', relj.id),
                                         'a_siglum', reli.siglum,
                                         'b_siglum', relj.siglum,
                                         'a_name', reli.corporate_name,
                                         'b_name', relj.corporate_name,
                                         'a_place', reli.place,
                                         'b_place', relj.place,
                                         'marc_tag', rela.marc_tag,
                                         'a_now_in_b', (rela.institution_a_id = i.id),
                                         'b_contains_a', (rela.institution_b_id = i.id)
                             ))
        FROM {dbname}.institutions_to_institutions AS rela
        LEFT JOIN {dbname}.institutions AS reli ON reli.id = rela.institution_a_id
        LEFT JOIN {dbname}.institutions AS relj ON relj.id = rela.institution_b_id
        WHERE rela.institution_a_id = i.id OR rela.institution_b_id = i.id
    ) AS institution_relationships,
    (SELECT JSON_ARRAYAGG(DISTINCT
                         JSON_OBJECT('place_id', CONCAT('place_', reli.id),
                                      'relationship', COALESCE(rela.relator_code, "xp"),
                                      'name', reli.name,
                                      'country', reli.country,
                                      'district', reli.district,
                                      'id', CAST(rela.id AS CHAR),
                                      'this_type', 'institution',
                                      'this_id', CONCAT('institution_', i.id)))
        FROM {dbname}.institutions_to_places AS rela
        LEFT JOIN {dbname}.places AS reli ON reli.id = rela.place_id
        WHERE rela.institution_id = i.id AND rela.marc_tag = '551'
   ) AS related_places,
    (SELECT JSON_ARRAYAGG(DISTINCT CONCAT('dobject_', do.digital_object_id))
        FROM {dbname}.digital_object_links AS do
        WHERE do.object_link_type = 'Institution'
            AND do.object_link_id = i.id
    ) AS digital_objects,
    (SELECT JSON_ARRAYAGG(DISTINCT ssi.relator_code)
        FROM {dbname}.sources_to_institutions AS ssi
        LEFT JOIN {dbname}.sources AS sss ON ssi.source_id = sss.id
        WHERE i.id = ssi.institution_id AND sss.wf_stage = 1
    ) AS source_relationships,
    ( SELECT EXISTS (
        SELECT 1 FROM {dbname}.people_to_institutions pi
        WHERE pi.marc_tag = '910' AND pi.institution_id = i.id

        UNION ALL

        SELECT 1 FROM {dbname}.sources_to_institutions si
        WHERE si.marc_tag = '910' AND si.institution_id = i.id
    )) AS is_contributing_project
FROM {dbname}.institutions AS i
WHERE i.siglum IS NOT NULL OR
    ((EXISTS (SELECT 1 FROM {dbname}.holdings_to_institutions AS hi WHERE hi.institution_id = i.id)
    OR EXISTS (SELECT 1 FROM {dbname}.institutions_to_institutions AS ii WHERE ii.institution_a_id = i.id)
    OR EXISTS (SELECT 1 FROM {dbname}.institutions_to_institutions AS ii WHERE ii.institution_b_id = i.id)
    OR EXISTS (SELECT 1 FROM {dbname}.people_to_institutions AS pi WHERE pi.institution_id = i.id)
    OR EXISTS (SELECT 1 FROM {dbname}.publications_to_institutions AS bi WHERE bi.institution_id = i.id)
    OR EXISTS (SELECT 1 FROM {dbname}.sources_to_institutions AS si WHERE si.institution_id = i.id))
    ) {id_where_clause}
GROUP BY i.id
ORDER BY i.id ASC;
"""  # noqa: S608

    curs.execute(sql)

    while rows := curs._cursor.fetchmany(cfg["mysql"]["resultsize"]):
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
            doc: dict[str, object] = create_institution_index_document(record, cfg)
        except RequiredFieldException:
            log.error(
                "A required field was not found, so this document was not indexed."
            )
            continue

        records_to_index.append(doc)

    check: bool = True if cfg["dry"] else submit_to_solr(list(records_to_index), cfg)

    if not check:
        log.error("There was an error submitting institutions to Solr")

    return check
