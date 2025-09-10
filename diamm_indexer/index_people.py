import logging
from collections.abc import Generator
from typing import Any

from psycopg.rows import dict_row

from diamm_indexer.helpers.db import postgres_pool
from diamm_indexer.records.person import (
    create_person_index_document,
    get_date_statement,
    get_name,
)
from indexer.helpers.solr import record_indexer, submit_to_solr
from indexer.helpers.utilities import parallelise, update_rism_document

log = logging.getLogger("muscat_indexer")


def _get_people(cfg: dict) -> Generator[list[dict[str, Any]]]:
    with postgres_pool.connection() as conn:
        curs = conn.cursor(row_factory=dict_row)
        curs.execute("""SELECT DISTINCT ddp.id AS id, ddp.last_name AS last_name,
                        ddp.first_name AS first_name, ddp.earliest_year AS earliest_year,
                        ddp.latest_year AS latest_year, ddp.earliest_year_approximate AS earliest_approx,
                        ddp.latest_year_approximate AS latest_approx,
                        (SELECT jsonb_agg(DISTINCT jsonb_build_object(
                                                      'id', ddos.id,
                                                      'siglum', ddoa.siglum,
                                                      'shelfmark', ddos.shelfmark,
                                                      'name', ddos.name,
                                                      'relationship_type_id', ddsr.relationship_type_id,
                                                      'relationship_type_name', ddsrt.name,
                                                      'relationship_uncertain', ddsr.uncertain
                                                   ))
                            FROM diamm_data_sourcerelationship ddsr
                            LEFT JOIN diamm_data_source AS ddos ON ddsr.source_id = ddos.id
                            LEFT JOIN diamm_data_archive AS ddoa ON ddos.archive_id = ddoa.id
                            LEFT JOIN diamm_data_sourcerelationshiptype AS ddsrt ON ddsr.relationship_type_id = ddsrt.id
                            WHERE ddsr.content_type_id = 37 AND ddsr.object_id = ddp.id
                        ) AS related_sources,
                        (SELECT jsonb_agg(DISTINCT jsonb_build_object(
                                                      'id', ddos.id,
                                                      'siglum', ddoa.siglum,
                                                      'shelfmark', ddos.shelfmark,
                                                      'name', ddos.name,
                                                      'relationship_type_id', '6',
                                                      'relationship_type_name', '',
                                                      'relationship_uncertain', ddsc.uncertain
                                                  ))
                            FROM diamm_data_sourcecopyist ddsc
                            LEFT JOIN diamm_data_source AS ddos ON ddsc.source_id = ddos.id
                            LEFT JOIN diamm_data_archive AS ddoa ON ddos.archive_id = ddoa.id
                            WHERE ddsc.content_type_id = 37 AND ddsc.object_id = ddp.id)
                        AS copied_sources
                        FROM diamm_data_person ddp
                        LEFT JOIN diamm_data_personidentifier ddpi ON ddpi.person_id = ddp.id
                        WHERE ddp.id != 4221 AND (ddpi.person_id IS NULL OR 1 NOT IN (
                            SELECT ddpi2.identifier_type FROM diamm_data_personidentifier ddpi2 WHERE ddpi2.person_id = ddp.id
                        ))
                        GROUP BY ddp.id
                        ORDER BY ddp.id;""")

        while rows := curs.fetchmany(size=500):
            yield rows


def _get_linked_diamm_people(cfg: dict) -> Generator[list[dict[str, Any]]]:
    with postgres_pool.connection() as conn:
        curs = conn.cursor(row_factory=dict_row)
        curs.execute("""SELECT DISTINCT ddp.id AS id, ddpi.identifier AS rism_id,ddp.last_name AS last_name,
                            ddp.first_name AS first_name, ddp.earliest_year AS earliest_year,
                            ddp.latest_year AS latest_year, ddp.earliest_year_approximate AS earliest_approx,
                            ddp.latest_year_approximate AS latest_approx, 'people' AS project_type,
                            (SELECT COUNT(DISTINCT ddi.source_id)
                             FROM diamm_data_item AS ddi
                                 LEFT JOIN diamm_data_compositioncomposer AS ddcc ON ddi.composition_id = ddcc.composition_id
                                 LEFT JOIN diamm_data_itemcomposer AS ddii ON ddi.id = ddii.item_id
                                 LEFT JOIN diamm_data_sourceauthority AS ddsa ON ddi.source_id = ddsa.source_id AND ddsa.identifier_type = 1
                             WHERE ddsa.id IS NULL AND (ddcc.composer_id = ddp.id OR ddii.composer_id = ddp.id)
                            ) AS source_count
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
        name: str = get_name(record)
        date_statement: str | None = get_date_statement(record)
        if not date_statement:
            continue

        full_name: str = f"{name} ({date_statement})" if date_statement else f"{name}"

        doc = update_rism_document(record, "diamm", "person", full_name, cfg)
        if not doc:
            continue
        records.append(doc)

    check: bool = True if cfg["dry"] else submit_to_solr(records, cfg)

    if not check:
        log.error("There was an error submitting people to Solr")

    return check
