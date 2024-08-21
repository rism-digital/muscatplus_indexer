import logging
from typing import Generator

from psycopg.rows import dict_row

from cantus_indexer.helpers.db import postgres_pool
from cantus_indexer.records.institution import create_institution_index_document
from indexer.helpers.solr import record_indexer, submit_to_solr
from indexer.helpers.utilities import parallelise, update_rism_document

log = logging.getLogger("muscat_indexer")


def _get_unlinked_cantus_institutions(cfg: dict) -> Generator[dict, None, None]:
    with postgres_pool.connection() as conn:
        curs = conn.cursor(row_factory=dict_row)
        # Only select institutions that have *published* sources attached to them.
        curs.execute("""SELECT DISTINCT cti.id AS id, cti.name AS name, cti.date_created AS created,
                    cti.date_updated AS updated, cti.city AS city, cti.country AS country
                    FROM main_app_institution AS cti
                    LEFT JOIN main_app_institutionidentifier AS ctii ON cti.id = ctii.institution_id
                    WHERE ctii.institution_id IS NULL AND
                      (SELECT COUNT(cts.holding_institution_id)
                       FROM main_app_source cts
                       WHERE cts.holding_institution_id = cti.id AND cts.published is TRUE) > 0""")

    while rows := curs.fetchmany(size=500):
        yield rows


def _get_linked_cantus_institutions(cfg: dict) -> Generator[dict, None, None]:
    with postgres_pool.connection() as conn:
        curs = conn.cursor(row_factory=dict_row)
        curs.execute("""SELECT DISTINCT cti.id AS id, ctii.identifier AS rism_id, cti.name AS name,
                'institution' AS project_type
                FROM main_app_institution AS cti
                LEFT JOIN main_app_institutionidentifier AS ctii ON cti.id = ctii.institution_id
                WHERE ctii.institution_id IS NOT NULL AND ctii.identifier_type = 1
                ORDER BY cti.id""")

        while rows := curs.fetchmany(size=500):
            yield rows


def update_institution_records_with_cantus_institutions(
    institutions: list, cfg: dict
) -> bool:
    log.info("Updating RISM institution records with Cantus info")
    records = []

    for record in institutions:
        label: str = record.get("name")
        doc = update_rism_document(record, "cantus", "institution", label, cfg)
        if not doc:
            continue
        records.append(doc)

    check: bool = True if cfg["dry"] else submit_to_solr(records, cfg)

    if not check:
        log.error("There was an error updating institution records in Solr")

    return check


def index_unlinked_cantus_institutions(cfg: dict) -> bool:
    institutions = _get_unlinked_cantus_institutions(cfg)
    parallelise(institutions, record_indexer, create_institution_index_document, cfg)

    return True


def update_linked_rism_institutions(cfg: dict) -> bool:
    institutions = _get_linked_cantus_institutions(cfg)
    parallelise(institutions, update_institution_records_with_cantus_institutions, cfg)

    return True


def index_institutions(cfg: dict) -> bool:
    res = True
    res |= index_unlinked_cantus_institutions(cfg)
    res |= update_linked_rism_institutions(cfg)

    return res
