import logging
from typing import Any, Generator

from psycopg.rows import dict_row

from diamm_indexer.helpers.db import postgres_pool
from diamm_indexer.records.organization import create_organization_index_document
from indexer.helpers.solr import record_indexer, submit_to_solr
from indexer.helpers.utilities import parallelise, update_rism_document

log = logging.getLogger("muscat_indexer")


def _get_organizations(cfg: dict) -> Generator[list[dict[str, Any]], None, None]:
    with postgres_pool.connection() as conn:
        curs = conn.cursor(row_factory=dict_row)
        curs.execute("""SELECT DISTINCT ddo.id AS id, ddo.name AS name, ddo.created AS created, ddo.updated AS updated,
                        (SELECT string_agg(DISTINCT
                                CONCAT(ddg1.name, '||',
                                       ddg2.name, '||',
                                       ddg2.id), '\n') AS location
                            FROM diamm_data_geographicarea ddg1
                            LEFT JOIN diamm_data_geographicarea ddg2 ON ddg1.parent_id = ddg2.id
                            WHERE ddg1.id = ddo.location_id AND ddg1.type = 1) AS location,
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
                             WHERE ddsr.content_type_id = 52 AND ddsr.object_id = ddo.id) AS related_sources,
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
                             WHERE ddsc.content_type_id = 52 AND ddsc.object_id = ddo.id) AS copied_sources
                        FROM diamm_data_organization ddo
                                 LEFT JOIN diamm_data_geographicarea ddg on ddo.location_id = ddg.id
                                 LEFT JOIN diamm_data_organizationidentifier ddoi ON ddoi.organization_id = ddo.id
                        WHERE ddoi.organization_id IS NULL OR 1 NOT IN (
                            SELECT ddoi2.identifier_type FROM diamm_data_organizationidentifier ddoi2 WHERE ddoi2.organization_id = ddo.id
                        )
                        GROUP BY ddo.id
                        ORDER BY ddo.id;""")

        while rows := curs.fetchmany(size=500):
            yield rows


def _get_linked_diamm_organizations(
    cfg: dict,
) -> Generator[list[dict[str, Any]], None, None]:
    with postgres_pool.connection() as conn:
        curs = conn.cursor(row_factory=dict_row)
        curs.execute("""SELECT DISTINCT ddo.id AS id, ddoi.identifier AS rism_id, ddo.name AS name,
                        'organizations' AS project_type
                        FROM diamm_data_organization ddo
                        LEFT JOIN diamm_data_organizationidentifier ddoi on ddo.id = ddoi.organization_id
                        WHERE ddoi.organization_id IS NOT NULL AND ddoi.identifier_type = 1
                        ORDER BY ddo.id;""")

        while rows := curs.fetchmany(size=500):
            yield rows


def index_organizations(cfg: dict) -> bool:
    org_groups = _get_organizations(cfg)
    parallelise(org_groups, record_indexer, create_organization_index_document, cfg)

    rism_orgs = _get_linked_diamm_organizations(cfg)
    parallelise(rism_orgs, update_institution_records_with_diamm_info, cfg)

    return True


def _get_linked_diamm_archives(
    cfg: dict,
) -> Generator[list[dict[str, Any]], None, None]:
    with postgres_pool.connection() as conn:
        curs = conn.cursor(row_factory=dict_row)
        curs.execute("""SELECT DISTINCT dda.id AS id, ddai.identifier AS rism_id, dda.name AS name,
                        'archives' AS project_type
                        FROM diamm_data_archive dda
                        LEFT JOIN diamm_data_archiveidentifier ddai on dda.id = ddai.archive_id
                        WHERE ddai.archive_id IS NOT NULL AND ddai.identifier_type = 1
                        ORDER BY dda.id;""")

        while rows := curs.fetchmany(size=500):
            yield rows


def update_archives(cfg: dict) -> bool:
    rism_archives = _get_linked_diamm_archives(cfg)
    parallelise(rism_archives, update_institution_records_with_diamm_info, cfg)

    return True


def update_institution_records_with_diamm_info(archives: list, cfg: dict) -> bool:
    log.info("Updating RISM institution records with DIAMM info")
    records = []

    for record in archives:
        label: str = record.get("name")
        doc = update_rism_document(record, "diamm", "institution", label, cfg)
        if not doc:
            continue
        records.append(doc)

    check: bool = True if cfg["dry"] else submit_to_solr(records, cfg)

    if not check:
        log.error("There was an error updating institution records in Solr")

    return check


def index_institutions(cfg: dict) -> bool:
    res = True
    res |= index_organizations(cfg)
    res |= update_archives(cfg)

    return res
