import logging
from typing import Generator

from psycopg.rows import dict_row

from diamm_indexer.helpers.db import postgres_pool
from diamm_indexer.records.source import create_source_index_documents
from indexer.exceptions import RequiredFieldException
from indexer.helpers.solr import submit_to_diamm_solr
from indexer.helpers.utilities import parallelise

log = logging.getLogger("muscat_indexer")


def _get_sources(cfg: dict) -> Generator[dict, None, None]:
    with postgres_pool.connection() as conn:
        curs = conn.cursor(row_factory=dict_row)
        curs.execute("""SELECT dds.id AS id, dds.name AS name, dds.shelfmark AS shelfmark, dds.start_date AS start_date, 
                        dds.end_date AS end_date, dds.date_statement AS date_statement, dds.measurements AS measurments,
                        dds.created AS created, dds.updated AS updated, dda.name AS archive_name, dda.siglum AS siglum, 
                        ddg.name AS city_name
                        FROM diamm_data_source dds
                        LEFT JOIN diamm_data_archive dda on dds.archive_id = dda.id
                        LEFT JOIN diamm_data_geographicarea ddg on dda.city_id = ddg.id
                        ORDER BY id;""")

        while rows := curs.fetchmany(size=500):
            yield rows


def index_sources(cfg: dict) -> bool:
    source_groups = _get_sources(cfg)
    parallelise(source_groups, index_source_groups, cfg)

    return True


def index_source_groups(sources: list, cfg: dict) -> bool:
    records = []

    for record in sources:
        try:
            docs = create_source_index_documents(record, cfg)
        except RequiredFieldException:
            log.error("Could not index source %s", record['id'])
            continue

        records.append(docs)

    return submit_to_diamm_solr(records, cfg)
