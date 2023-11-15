import logging
from typing import Generator
from psycopg.rows import dict_row
from diamm_indexer.helpers.db import postgres_pool
from diamm_indexer.helpers.solr import record_indexer
from diamm_indexer.records.person import create_person_index_document, update_rism_person_document
from indexer.helpers.solr import submit_to_solr
from indexer.helpers.utilities import parallelise

log = logging.getLogger("muscat_indexer")


def _get_people(cfg: dict) -> Generator[dict, None, None]:
    with postgres_pool.connection() as conn:
        curs = conn.cursor(row_factory=dict_row)
        curs.execute("""SELECT DISTINCT ddp.id AS id, ddp.last_name AS last_name,
                ddp.first_name AS first_name, ddp.earliest_year AS earliest_year,
                ddp.latest_year AS latest_year, ddp.earliest_year_approximate AS earliest_approx,
                ddp.latest_year_approximate AS latest_approx,
                (SELECT string_agg(DISTINCT
                                   CONCAT(ddoa.siglum, '||',
                                          ddos.shelfmark, '||',
                                          ddos.name, '||',
                                          ddsr.relationship_type_id, '||',
                                          ddsrt.name , '||',
                                          ddsr.uncertain, '||',
                                          ddos.id), '\n') AS sources
                 FROM diamm_data_sourcerelationship ddsr
                          LEFT JOIN diamm_data_source AS ddos ON ddsr.source_id = ddos.id
                          LEFT JOIN diamm_data_archive AS ddoa ON ddos.archive_id = ddoa.id
                          LEFT JOIN diamm_data_sourcerelationshiptype AS ddsrt ON ddsr.relationship_type_id = ddsrt.id
                 WHERE ddsr.content_type_id = 37 AND ddsr.object_id = ddp.id) AS related_sources,
                (SELECT string_agg(DISTINCT
                                   CONCAT(ddoa.siglum, '||',
                                          ddos.shelfmark, '||',
                                          ddos.name, '||',
                                          '6', '||', '||',
                                          ddsc.uncertain, '||',
                                          ddos.id), '\n') AS sources
                 FROM diamm_data_sourcecopyist ddsc
                          LEFT JOIN diamm_data_source AS ddos ON ddsc.source_id = ddos.id
                          LEFT JOIN diamm_data_archive AS ddoa ON ddos.archive_id = ddoa.id
                 WHERE ddsc.content_type_id = 37 AND ddsc.object_id = ddp.id) AS copied_sources
FROM diamm_data_person ddp
         LEFT JOIN diamm_data_personidentifier ddpi ON ddpi.person_id = ddp.id
WHERE ddp.id != 4221 AND (ddpi.person_id IS NULL OR 1 NOT IN (
    SELECT ddpi2.identifier_type FROM diamm_data_personidentifier ddpi2 WHERE ddpi2.person_id = ddp.id
))
GROUP BY ddp.id
ORDER BY ddp.id;""")

        while rows := curs.fetchmany(size=500):
            yield rows


def _get_linked_diamm_people(cfg: dict) -> Generator[dict, None, None]:
    with postgres_pool.connection() as conn:
        curs = conn.cursor(row_factory=dict_row)
        curs.execute("""SELECT DISTINCT ddp.id AS id, ddpi.identifier AS rism_id,ddp.last_name AS last_name,
                            ddp.first_name AS first_name, ddp.earliest_year AS earliest_year,
                            ddp.latest_year AS latest_year, ddp.earliest_year_approximate AS earliest_approx,
                            ddp.latest_year_approximate AS latest_approx,
                            'people' AS project_type
                        FROM diamm_data_person ddp
                        LEFT JOIN diamm_data_personidentifier ddpi on ddp.id = ddpi.person_id
                        WHERE ddpi.person_id IS NOT NULL AND ddpi.identifier_type = 1
                        ORDER BY ddp.id;""")

        while rows := curs.fetchmany(size=500):
            yield rows


def index_people(cfg: dict) -> bool:
    people_groups = _get_people(cfg)
    parallelise(people_groups, record_indexer, create_person_index_document, cfg)

    rism_people = _get_linked_diamm_people(cfg)
    parallelise(rism_people, update_person_records_with_diamm_info, cfg)
    return True


def update_person_records_with_diamm_info(people: list, cfg: dict) -> bool:
    log.info("Updating RISM person records with DIAMM info")
    records = []

    for record in people:
        doc = update_rism_person_document(record, cfg)
        if not doc:
            continue
        records.append(doc)
    check: bool
    if cfg["dry"]:
        check = True
    else:
        check = submit_to_solr(records, cfg)

    if not check:
        log.error("There was an error submitting people to Solr")

    return check