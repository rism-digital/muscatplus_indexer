import logging
from collections import deque
from collections.abc import Generator

from indexer.helpers.db import mysql_pool
from indexer.helpers.solr import submit_to_solr
from indexer.helpers.utilities import parallelise
from indexer.records.person import create_person_index_document

log = logging.getLogger("muscat_indexer")


def _get_people_groups(cfg: dict) -> Generator[dict, None, None]:
    conn = mysql_pool.connection()
    curs = conn.cursor()
    dbname: str = cfg["mysql"]["database"]

    id_where_clause: str = ""
    if "id" in cfg:
        id_where_clause = f"AND p.id = {cfg['id']}"

    sql_statement = f"""WITH person_work_nodes AS (
                            SELECT wnp.person_id AS person_id,
                                   JSON_OBJECT('count', (SELECT COUNT(swn1.source_id) FROM {dbname}.sources_to_work_nodes AS swn1 WHERE swn1.work_node_id = wn.id),
                                               'marc_source', wn.marc_source) AS json_object
                            FROM {dbname}.sources_to_work_nodes AS swn
                                LEFT JOIN {dbname}.work_nodes_to_people AS wnp ON swn.work_node_id = wnp.work_node_id
                            LEFT JOIN {dbname}.work_nodes AS wn ON wnp.work_node_id = wn.id
                        GROUP BY wn.id, wn.title
                        ORDER BY wn.title ASC
                            )

                        SELECT p.id AS id, p.marc_source AS marc_source,
                               p.created_at AS created, p.updated_at AS updated,
                               (SELECT COUNT(DISTINCT sp.source_id)
                                FROM {dbname}.sources_to_people sp
                                LEFT JOIN {dbname}.sources ss ON sp.source_id = ss.id
                                WHERE sp.person_id = p.id AND (ss.wf_stage IS NULL OR ss.wf_stage = 1)
                               )
                                   +
                               (
                                   SELECT COUNT(DISTINCT ho.source_id)
                                   FROM {dbname}.holdings ho
                                   LEFT JOIN {dbname}.holdings_to_people hp ON ho.id = hp.holding_id
                                   WHERE hp.person_id = p.id
                               ) AS source_count,
                               (SELECT GROUP_CONCAT(DISTINCT COALESCE(ssp.relator_code, 'cre') SEPARATOR ',')
                                FROM {dbname}.sources_to_people AS ssp
                                LEFT JOIN {dbname}.sources AS sss ON ssp.source_id = sss.id
                                WHERE p.id = ssp.person_id AND sss.wf_stage = 1)
                                   AS source_relationships,
                               (SELECT GROUP_CONCAT(DISTINCT do.digital_object_id SEPARATOR ',')
                                FROM {dbname}.digital_object_links AS do
                                WHERE do.object_link_type = 'Person' AND do.object_link_id = p.id)
                                   AS digital_objects,
                               (SELECT JSON_ARRAYAGG(ww.json_object) FROM person_work_nodes ww WHERE ww.person_id = p.id) AS work_nodes
                        FROM {dbname}.people AS p
                        WHERE
                            EXISTS (SELECT 1 FROM {dbname}.people_to_institutions pi WHERE pi.person_id = p.id)
                            OR EXISTS (SELECT 1 FROM {dbname}.people_to_people pp1 WHERE pp1.person_a_id = p.id)
                            OR EXISTS (SELECT 1 FROM {dbname}.people_to_people pp2 WHERE pp2.person_b_id = p.id)
                            OR EXISTS (SELECT 1 FROM {dbname}.sources_to_people sp WHERE sp.person_id = p.id)
                            OR EXISTS (SELECT 1 FROM {dbname}.holdings_to_people hp WHERE hp.person_id = p.id)
                            OR EXISTS (SELECT 1 FROM {dbname}.institutions_to_people ip WHERE ip.person_id = p.id)
                            OR EXISTS (SELECT 1 FROM {dbname}.people_to_publications pubp WHERE pubp.person_id = p.id)
                            OR EXISTS (SELECT 1 FROM {dbname}.publications_to_people ppub WHERE ppub.person_id = p.id)
                            {id_where_clause};"""  # noqa: S608


    curs.execute(sql_statement)

    while rows := curs._cursor.fetchmany(cfg["mysql"]["resultsize"]):  # noqa
        yield rows

    curs.close()
    conn.close()


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
