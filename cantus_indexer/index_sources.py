import logging
from collections import deque
from typing import Generator

from psycopg.rows import dict_row

from cantus_indexer.helpers.db import postgres_pool
from cantus_indexer.records.source import create_source_index_documents
from indexer.exceptions import RequiredFieldException
from indexer.helpers.solr import submit_to_solr
from indexer.helpers.utilities import parallelise

log = logging.getLogger("muscat_indexer")


def _get_sources(cfg: dict) -> Generator[dict, None, None]:
    with postgres_pool.connection() as conn:
        curs = conn.cursor(row_factory=dict_row)
        curs.execute("""SELECT cts.id AS id, cts.shelfmark AS shelfmark, cts.date AS source_date, cts.summary AS source_summary,
                    cts.description AS html_source_description, cts.image_link AS digital_images,
                    cts.date_created AS created, cts.date_updated AS updated,
                    cti.name AS institution_name, cti.city AS institution_city, cti.country AS institution_country,
                    cti.siglum AS institution_siglum, cti.id AS institution_id,
                   (SELECT string_agg(ctii.identifier, '\n') FROM main_app_institutionidentifier ctii
                        WHERE ctii.institution_id = cti.id AND ctii.identifier_type = 1) AS institution_rism_ids,
                   (SELECT string_agg(ctc.name, ', ') FROM main_app_source_century ctsc
                        LEFT JOIN main_app_century ctc ON ctsc.century_id = ctc.id
                        WHERE ctsc.source_id = cts.id) AS source_century,
                   (SELECT string_agg(ctn.name, ', ') FROM main_app_source_notation ctsn
                        LEFT JOIN main_app_notation ctn ON ctsn.notation_id = ctn.id
                        WHERE ctsn.source_id = cts.id) AS source_notation,
                   (SELECT string_agg(ctc.incipit, '\n') FROM main_app_chant ctc
                        WHERE ctc.source_id = cts.id) AS source_incipits
                    FROM main_app_source cts
                    LEFT JOIN main_app_institution cti ON cti.id = cts.holding_institution_id
                    WHERE cts.published is TRUE
                    ORDER BY cts.id""")

        while rows := curs.fetchmany(size=cfg["postgres"]["resultsize"]):
            yield rows


def index_sources(cfg: dict) -> bool:
    source_groups = _get_sources(cfg)
    parallelise(source_groups, index_source_groups, cfg)

    return True


def index_source_groups(sources: list, cfg: dict) -> bool:
    records_to_index: deque = deque()

    for record in sources:
        try:
            docs = create_source_index_documents(record, cfg)
        except RequiredFieldException:
            log.error("Could not index source %s", record["id"])
            continue

        records_to_index.extend(docs)

    check: bool = True if cfg["dry"] else submit_to_solr(list(records_to_index), cfg)

    if not check:
        log.error("There was an error submitting Cantus Sources to Solr")

    return check
