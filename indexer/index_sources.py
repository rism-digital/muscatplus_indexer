import gc
import logging
from collections import deque
from collections.abc import Generator

from indexer.exceptions import RequiredFieldException
from indexer.helpers.db import mysql_pool
from indexer.helpers.solr import submit_to_solr
from indexer.helpers.utilities import parallelise
from indexer.records.source import create_source_index_documents

log = logging.getLogger("muscat_indexer")


def _get_sources(cfg: dict) -> Generator[dict]:
    log.info("Getting list of sources to index")
    conn = mysql_pool.connection()
    curs = conn.cursor()
    dbname: str = cfg["mysql"]["database"]

    id_where_clause: str = ""
    if "id" in cfg:
        id_where_clause = f"AND child.id = {cfg['id']}"

    sql_query: str = f"""
    SELECT child.id AS id, child.title AS title, child.std_title AS std_title,
        child.source_id AS source_id, child.marc_source AS marc_source, child.composer AS creator_name,
        child.created_at AS created, child.updated_at AS updated, parent.marc_source AS parent_marc_source,
        child.record_type AS record_type, parent.std_title AS parent_title, parent.shelf_mark AS parent_shelfmark,
        parent.lib_siglum AS parent_siglum, parent.record_type AS parent_record_type,
        COUNT(DISTINCT h.id) AS child_holdings_count,
        COUNT(DISTINCT hp.id) AS parent_holdings_count,
        (SELECT COUNT(ss.id) FROM {dbname}.sources AS ss WHERE ss.source_id = child.id) as child_count,
        (SELECT JSON_ARRAYAGG(DISTINCT srm2.marc_source)
            FROM {dbname}.sources AS srm2
            WHERE srm2.source_id IS NOT NULL AND srm2.source_id = child.id
        ) AS child_marc_records,
        (SELECT JSON_ARRAYAGG(DISTINCT
                             JSON_OBJECT('id', CONCAT('institution_', ins.id),
                                        'name', ins.corporate_name,
                                        'relator_code', ssi.relator_code,
                                        'siglum', ins.siglum,
                                        'place', ins.place))
            FROM {dbname}.sources_to_institutions ssi
            LEFT JOIN {dbname}.institutions ins ON ssi.institution_id = ins.id
            WHERE ssi.marc_tag = '852' AND child.id = ssi.source_id
        ) AS ms_holding_institutions,
        (SELECT JSON_ARRAYAGG(DISTINCT
                             JSON_OBJECT('relator_code', stos.relator_code,
                                         'marc_source', sours.marc_source))
            FROM {dbname}.sources_to_sources AS stos
            LEFT JOIN {dbname}.sources AS sours ON stos.source_b_id = sours.id
            WHERE marc_tag = '787' AND source_a_id = child.id
        ) AS related_sources,
        (SELECT JSON_ARRAYAGG(DISTINCT CONCAT('dobject_', do.digital_object_id))
                FROM {dbname}.digital_object_links AS do
                WHERE do.object_link_type = 'Source' AND do.object_link_id = child.id
        ) AS digital_objects,
        -- NB: Only one work node is permitted on a source, even though this technically allows for more. To ensure we only have 0 or 1 record, a LIMIT clause is added.
        (SELECT JSON_OBJECT('id', CONCAT('work_node_', wn.id),
                           'marc_source', wn.marc_source)
            FROM {dbname}.sources_to_work_nodes AS swn
            LEFT JOIN {dbname}.work_nodes AS wn ON swn.work_node_id = wn.id
            WHERE swn.source_id = child.id LIMIT 1
        ) AS work_node,
        (SELECT (JSON_ARRAYAGG(DISTINCT CONCAT('work_', sw.work_id)))
            FROM {dbname}.sources_to_works AS sw
            LEFT JOIN {dbname}.sources AS ss ON sw.source_id = ss.id
            WHERE sw.source_id = child.id AND ss.wf_stage = 1
        ) AS work_ids,
        (SELECT JSON_ARRAYAGG(DISTINCT
                             JSON_OBJECT('lib_siglum', h2.lib_siglum,
                                         'marc_source', h2.marc_source))
            FROM {dbname}.holdings h2 WHERE h2.source_id = child.id
        ) AS holdings,
        (SELECT JSON_ARRAYAGG(DISTINCT
                             JSON_OBJECT('lib_siglum', hp2.lib_siglum,
                                         'marc_source', hp2.marc_source))
            FROM {dbname}.holdings hp2 WHERE hp2.source_id = parent.id
        ) AS parent_holdings,
        (SELECT JSON_ARRAYAGG(DISTINCT
                                JSON_OBJECT('id', pub.id,
                                     'author', pub.author,
                                     'title', pub.title,
                                     'journal', pub.journal,
                                     'date', pub.date,
                                     'place', pub.place,
                                     'short_name', pub.short_name,
                                     'marc_source', pub.marc_source))
            FROM {dbname}.sources_to_publications spt
            LEFT JOIN {dbname}.publications pub ON spt.publication_id = pub.id
            WHERE spt.source_id = child.id
        ) AS publication_entries,
        (SELECT JSON_ARRAYAGG(DISTINCT
                    JSON_OBJECT('id', CONCAT('person_', p2.id),
                               'name', p2.full_name,
                               'life_dates', p2.life_dates,
                               'alternate_names', p2.alternate_names))
            FROM {dbname}.people p2
            LEFT JOIN {dbname}.sources_to_people sp ON p2.id = sp.person_id
            WHERE sp.source_id = child.id
        ) AS people,
        (SELECT JSON_ARRAYAGG(DISTINCT st2.alternate_terms)
            FROM {dbname}.standard_terms st2
            LEFT JOIN {dbname}.sources_to_standard_terms sst2 ON st2.id = sst2.standard_term_id
            WHERE sst2.source_id = child.id AND st2.alternate_terms != ''
        ) AS alt_standard_terms,
        (SELECT JSON_ARRAYAGG(DISTINCT
                             JSON_OBJECT('place_id', CONCAT('place_', reli.id),
                                          'relationship', COALESCE(rela.relator_code, "xp"),
                                          'name', reli.name,
                                          'country', reli.country,
                                          'district', reli.district,
                                          'id', rela.id,
                                          'this_type', 'person',
                                          'this_id', CONCAT('person_', p.id)))
            FROM {dbname}.sources_to_places AS rela
            LEFT JOIN {dbname}.places AS reli ON reli.id = rela.place_id
            WHERE rela.source_id = child.id AND rela.marc_tag = '651'
       ) AS locations_of_performances
FROM {dbname}.sources AS child
    LEFT JOIN {dbname}.sources AS parent ON parent.id = child.source_id
    LEFT JOIN {dbname}.holdings h on child.id = h.source_id
    LEFT JOIN {dbname}.holdings hp on parent.id = hp.source_id
    LEFT JOIN {dbname}.sources_to_people sp on sp.source_id = child.id
    LEFT JOIN {dbname}.people p on sp.person_id = p.id
WHERE child.wf_stage = 1 {id_where_clause}
GROUP BY child.id
ORDER BY child.id asc;"""  # noqa: S608

    curs.execute(sql_query)

    while rows := curs._cursor.fetchmany(cfg["mysql"]["resultsize"]):  # noqa
        yield rows

    curs.close()
    conn.close()


def index_sources(cfg: dict) -> bool:
    log.info("Indexing sources")
    source_groups = _get_sources(cfg)
    parallelise(source_groups, index_source_groups, cfg)

    return True


def index_source_groups(sources: list, cfg: dict) -> bool:
    log.info("Indexing Source Group")
    records_to_index: deque = deque()

    for record in sources:
        try:
            docs = create_source_index_documents(record, cfg)
        except RequiredFieldException:
            log.critical("Could not index source %s", record["id"])
            continue
        log.debug("Appending source document")
        records_to_index.extend(docs)

    check: bool = True if cfg["dry"] else submit_to_solr(list(records_to_index), cfg)

    if not check:
        log.error("There was an error submitting sources to Solr")

    del sources
    del records_to_index

    gc.collect()

    return check
