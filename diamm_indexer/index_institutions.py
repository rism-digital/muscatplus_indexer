import logging
from typing import Generator

from psycopg.rows import dict_row

from diamm_indexer.helpers.db import postgres_pool
from diamm_indexer.helpers.solr import record_indexer
from diamm_indexer.records.organization import create_organization_index_document, update_rism_institution_document
from indexer.helpers.solr import submit_to_solr
from indexer.helpers.utilities import parallelise

log = logging.getLogger("muscat_indexer")


def _get_organizations(cfg: dict) -> Generator[dict, None, None]:
    with postgres_pool.connection() as conn:
        curs = conn.cursor(row_factory=dict_row)
        curs.execute("""SELECT DISTINCT ddo.id AS id, ddo.name AS name, ddo.created AS created, ddo.updated AS updated,
            (SELECT name FROM diamm_data_geographicarea ddg WHERE ddg.id = ddo.location_id AND ddg.type = 1) AS city_name
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


def _get_linked_diamm_organizations(cfg: dict) -> Generator[dict, None, None]:
    with postgres_pool.connection() as conn:
        curs = conn.cursor(row_factory=dict_row)
        curs.execute("""SELECT DISTINCT ddo.id AS id, ddoi.identifier AS rism_id
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
    parallelise(rism_orgs, update_institution_records_with_diamm_organizations, cfg)

    return True


def update_institution_records_with_diamm_organizations(orgs: list, cfg: dict) -> bool:
    log.info("Updating RISM organization records with DIAMM info")
    records = []
    for record in orgs:
        doc = update_rism_institution_document(record, cfg)
        if not doc:
            continue
        records.append(doc)

    check: bool
    if cfg["dry"]:
        check = True
    else:
        check = submit_to_solr(records, cfg)

    if not check:
        log.error("There was an error updating institution records with DIAMM orgs")

    return check


def _get_linked_diamm_archives(cfg: dict) -> Generator[dict, None, None]:
    with postgres_pool.connection() as conn:
        curs = conn.cursor(row_factory=dict_row)
        curs.execute("""SELECT DISTINCT dda.id AS id, ddai.identifier AS rism_id
                        FROM diamm_data_archive dda
                        LEFT JOIN diamm_data_archiveidentifier ddai on dda.id = ddai.archive_id
                        WHERE ddai.archive_id IS NOT NULL AND ddai.identifier_type = 1
                        ORDER BY dda.id;""")

        while rows := curs.fetchmany(size=500):
            yield rows


def update_archives(cfg: dict) -> bool:
    rism_archives = _get_linked_diamm_archives(cfg)
    parallelise(rism_archives, update_institution_records_with_diamm_archives, cfg)

    return True


def update_institution_records_with_diamm_archives(archives: list, cfg: dict) -> bool:
    log.info("Updating RISM archive records with DIAMM info")
    records = []

    for record in archives:
        doc = update_rism_institution_document(record, cfg)
        if not doc:
            continue
        records.append(doc)

    check: bool
    if cfg["dry"]:
        check = True
    else:
        check = submit_to_solr(records, cfg)

    if not check:
        log.error("There was an error updating institution records in Solr")

    return check


def index_institutions(cfg: dict) -> bool:
    res = True
    res |= index_organizations(cfg)
    res |= update_archives(cfg)

    return res

