import logging
from typing import Generator

from psycopg.rows import dict_row

from diamm_indexer.helpers.db import postgres_pool
from diamm_indexer.helpers.solr import record_indexer
from diamm_indexer.records.archive import create_archive_index_document
from diamm_indexer.records.organization import create_organization_index_document
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


def _get_archives(cfg: dict):
    with postgres_pool.connection() as conn:
        curs = conn.cursor(row_factory=dict_row)
        curs.execute("""SELECT dda.id AS id, dda.name AS name, dda.created AS created, dda.updated AS updated,
                        dda.siglum AS siglum,
                        (SELECT name FROM diamm_data_geographicarea ddg WHERE ddg.id = dda.city_id) AS city_name,
                        (SELECT COUNT(*) FROM diamm_data_source dds WHERE dds.archive_id = dda.id) AS source_count,
                        (SELECT identifier
                            FROM diamm_data_archiveidentifier ddai
                            WHERE ddai.archive_id = dda.id AND ddai.identifier_type = 1
                            LIMIT 1) AS rism_identifier
                        FROM diamm_data_archive dda
                        ORDER BY dda.id;""")

        while rows := curs.fetchmany(size=500):
            yield rows


def index_archives(cfg: dict) -> bool:
    archive_groups = _get_archives(cfg)
    parallelise(archive_groups, record_indexer, create_archive_index_document, cfg)

    return True


def index_institutions(cfg: dict) -> bool:
    res = True
    res |= index_organizations(cfg)
    res |= index_archives(cfg)

    return res

