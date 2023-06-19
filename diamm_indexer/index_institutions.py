import logging
from typing import Generator, Callable

from psycopg.rows import dict_row

from diamm_indexer.helpers.db import postgres_pool
from diamm_indexer.records.archive import create_archive_index_document
from diamm_indexer.records.organization import create_organization_index_document
from indexer.exceptions import RequiredFieldException
from indexer.helpers.db import mysql_pool
from indexer.helpers.solr import submit_to_diamm_solr
from indexer.helpers.utilities import parallelise

log = logging.getLogger("muscat_indexer")


def _get_organizations(cfg: dict) -> Generator[dict, None, None]:
    with postgres_pool.connection() as conn:
        curs = conn.cursor(row_factory=dict_row)
        curs.execute("""SELECT ddo.id AS id, ddo.name AS name, ddo.created AS created, ddo.updated AS updated,
                        (SELECT name FROM diamm_data_geographicarea ddg WHERE ddg.id = ddo.location_id AND ddg.type = 1) AS city_name
                        FROM diamm_data_organization ddo
                        LEFT JOIN diamm_data_geographicarea ddg on ddo.location_id = ddg.id
                        ORDER BY ddo.id;""")

        while rows := curs.fetchmany(size=500):
            yield rows


def index_organizations(cfg: dict) -> bool:
    org_groups = _get_organizations(cfg)
    parallelise(org_groups, record_indexer, create_organization_index_document, cfg)

    return True


def record_indexer(records: list, converter: Callable, cfg: dict) -> bool:
    idx_records = []

    for record in records:
        try:
            doc = converter(record, cfg)
        except RequiredFieldException:
            log.error("Could not index institution %s", record['id'])
            continue

        idx_records.append(doc)

    return submit_to_diamm_solr(idx_records, cfg)


def _get_archives(cfg: dict):
    with postgres_pool.connection() as conn:
        curs = conn.cursor(row_factory=dict_row)
        curs.execute("""SELECT dda.id AS id, dda.name AS name, dda.created AS created, dda.updated AS updated,
                        dda.siglum AS siglum,
                        (SELECT name FROM diamm_data_geographicarea ddg WHERE ddg.id = dda.city_id) AS city_name,
                        (SELECT COUNT(*) FROM diamm_data_source dds WHERE dds.archive_id = dda.id) AS source_count
                        FROM diamm_data_archive dda
                        ORDER BY dda.id;""")

        while rows := curs.fetchmany(size=500):
            yield rows


def index_archives(cfg: dict) -> bool:
    archive_groups = _get_archives(cfg)
    parallelise(archive_groups, record_indexer, create_archive_index_document, cfg)

    return True


def get_rism_archive_id(cfg: dict) -> dict:
    """
    Fetches the list of sigla and IDs from the Muscat database so we can use
    these to look up the RISM IDs.
    :param cfg:
    :return:
    """
    conn = mysql_pool.connection()
    curs = conn.cursor()

    curs.execute("""SELECT id, siglum 
                    FROM muscat_development.institutions
                    WHERE siglum IS NOT NULL;""")

    res = curs.fetchall()

    return {record["siglum"]: record["id"] for record in res if record["siglum"]}


def index_institutions(cfg: dict) -> bool:
    res = True
    res |= index_organizations(cfg)

    rism_sigla_lookup = get_rism_archive_id(cfg)
    cfg["sigla_lookup"] = rism_sigla_lookup
    res |= index_archives(cfg)

    return res

