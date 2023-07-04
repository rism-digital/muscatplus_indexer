from typing import Generator
from psycopg.rows import dict_row
from diamm_indexer.helpers.db import postgres_pool
from diamm_indexer.helpers.solr import record_indexer
from diamm_indexer.records.person import create_person_index_document
from indexer.helpers.utilities import parallelise


def _get_people(cfg: dict) -> Generator[dict, None, None]:
    with postgres_pool.connection() as conn:
        curs = conn.cursor(row_factory=dict_row)
        curs.execute("""SELECT ddp.id AS id, ddp.last_name AS last_name,
                        ddp.first_name AS first_name, ddp.earliest_year AS earliest_year,
                        ddp.latest_year AS latest_year, ddp.earliest_year_approximate AS earliest_approx,
                        ddp.latest_year_approximate AS latest_approx
                        FROM diamm_data_person ddp
                        WHERE ddp.id != 4221 
                        ORDER BY ddp.last_name ASC;""")

        while rows := curs.fetchmany(size=500):
            yield rows


def index_people(cfg: dict) -> bool:
    people_groups = _get_people(cfg)
    parallelise(people_groups, record_indexer, create_person_index_document, cfg)
    return True
