import logging
from collections import deque
from collections.abc import Generator

from indexer.helpers.db import postgres_pool, server_side_cursor
from indexer.helpers.solr import submit_to_solr
from indexer.helpers.utilities import parallelise
from indexer.records.person import create_person_index_document

log = logging.getLogger("muscat_indexer")


def _get_people_groups(cfg: dict) -> Generator[dict]:
    dbname = "public"

    record_id = int(cfg["id"]) if "id" in cfg else None

    sql_statement = f"""WITH work_node_source_counts AS (
                            SELECT swn.work_node_id, COUNT(*) AS source_count
                            FROM {dbname}.sources_to_work_nodes AS swn
                            GROUP BY swn.work_node_id
                        ),
                        person_work_nodes AS (
                            SELECT wnp.person_id AS person_id,
                                   jsonb_build_object('count', wsc.source_count,
                                                      'marc_source', wn.marc_source) AS json_object
                            FROM {dbname}.work_nodes_to_people AS wnp
                            JOIN work_node_source_counts AS wsc ON wsc.work_node_id = wnp.work_node_id
                            JOIN {dbname}.work_nodes AS wn ON wn.id = wnp.work_node_id
                        )

                        SELECT p.id AS id, p.marc_source AS marc_source,
                               p.created_at AS created, p.updated_at AS updated,
                               (SELECT jsonb_agg(DISTINCT
                                                    jsonb_build_object('id', CONCAT('source_', sss.id),
                                                                'rel', COALESCE(ssp.relator_code, 'cre')))
                                   FROM {dbname}.sources_to_people AS ssp
                                   LEFT JOIN {dbname}.sources AS sss ON ssp.source_id = sss.id
                                   WHERE p.id = ssp.person_id
                                     AND sss.wf_stage = 1
                                     AND p.id != 30004985
                               ) AS source_relationships,
                               (SELECT jsonb_agg(DISTINCT
                                                    jsonb_build_object('id', CONCAT('holding_', hhp.holding_id),
                                                                'source_id', CONCAT('source_', hhs.id),
                                                                'rel', COALESCE(hhp.relator_code, 'oth')))
                                   FROM {dbname}.holdings AS hhr
                                   LEFT JOIN {dbname}.sources AS hhs ON hhr.source_id = hhs.id
                                   LEFT JOIN {dbname}.holdings_to_people AS hhp ON hhr.id = hhp.holding_id
                                   WHERE p.id = hhp.person_id
                                     AND hhs.wf_stage = 1
                                     AND p.id != 30004985
                               ) AS holding_relationships,
                               (SELECT jsonb_agg(DISTINCT CONCAT('dobject_', dol.digital_object_id))
                                    FROM {dbname}.digital_object_links AS dol
                                    WHERE dol.object_link_type = 'Person' AND dol.object_link_id = p.id
                               ) AS digital_objects,
                               (SELECT jsonb_agg(DISTINCT
                                                     jsonb_build_object('id', CONCAT('publication_', pub.id),
                                                                     'author', pub.author,
                                                                     'title', pub.title,
                                                                     'journal', pub.journal,
                                                                     'date', pub.date,
                                                                     'place', pub.place,
                                                                     'short_name', pub.short_name,
                                                                     'marc_source', pub.marc_source,
                                                                     'catalogue_type', pub.work_catalogue))
                                    FROM {dbname}.publications_to_people ppt2
                                    LEFT JOIN {dbname}.publications pub ON ppt2.publication_id = pub.id
                                    WHERE ppt2.person_id = p.id
                                      AND ppt2.marc_tag = '700'
                                      AND ppt2.relator_code = 'att'
                                      AND pub.work_catalogue IN (2, 3)
                               ) AS work_catalogues,
                               (SELECT jsonb_agg(DISTINCT
                                                      jsonb_build_object('id', CONCAT('institution_', reli.id),
                                                                  'siglum', reli.siglum,
                                                                  'name', reli.corporate_name,
                                                                  'city', reli.place))
                                    FROM {dbname}.people_to_institutions AS rela
                                    LEFT JOIN {dbname}.institutions AS reli ON reli.id = rela.institution_id
                                    WHERE rela.person_id = p.id AND rela.marc_tag = '510'
                               ) AS related_institutions,
                               (SELECT jsonb_agg(DISTINCT
                                                     jsonb_build_object('place_id', CONCAT('place_', reli.id),
                                                                  'relationship', COALESCE(rela.relator_code, 'xp'),
                                                                  'name', reli.name,
                                                                  'country', reli.country,
                                                                  'district', reli.district,
                                                                  'id', rela.id::text,
                                                                  'this_type', 'person',
                                                                  'this_id', CONCAT('person_', p.id)))
                                    FROM {dbname}.people_to_places AS rela
                                    LEFT JOIN {dbname}.places AS reli ON reli.id = rela.place_id
                                    WHERE rela.person_id = p.id AND rela.marc_tag = '551'
                               ) AS related_places,
                               (SELECT jsonb_agg(ww.json_object)
                                    FROM person_work_nodes ww
                                    WHERE ww.person_id = p.id
                               ) AS work_nodes,
                               (SELECT jsonb_agg(DISTINCT
                                            jsonb_build_object('id', (pub.id::text),
                                                 'author', pub.author,
                                                 'title', pub.title,
                                                 'journal', pub.journal,
                                                 'date', pub.date,
                                                 'place', pub.place,
                                                 'short_name', pub.short_name,
                                                 'marc_source', pub.marc_source))
                                    FROM {dbname}.people_to_publications wpt
                                    LEFT JOIN {dbname}.publications pub ON wpt.publication_id = pub.id
                                    WHERE wpt.person_id = p.id AND wpt.marc_tag = '670'
                               ) AS source_data_found
                        FROM {dbname}.people AS p
                        WHERE (%s::bigint IS NULL OR p.id = %s);"""  # noqa: S608

    with postgres_pool.connection() as conn, server_side_cursor(conn, "people") as curs:
            curs.execute(sql_statement, (record_id, record_id))
            while rows := curs.fetchmany(cfg["postgres"]["resultsize"]):
                yield rows


def index_people(cfg: dict) -> bool:
    people_groups = _get_people_groups(cfg)
    parallelise(people_groups, index_people_groups, cfg)

    return True


def index_people_groups(people: list, cfg: dict) -> bool:
    log.info("Indexing People")
    records_to_index: deque = deque()

    for record in people:
        doc = create_person_index_document(record, cfg)
        records_to_index.append(doc)

    check: bool = True if cfg["dry"] else submit_to_solr(list(records_to_index), cfg)

    if not check:
        log.error("There was an error submitting people to Solr")

    return check
