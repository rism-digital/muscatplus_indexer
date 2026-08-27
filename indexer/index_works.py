import logging
from collections.abc import Generator

from indexer.exceptions import RequiredFieldException
from indexer.helpers.db import postgres_pool, server_side_cursor
from indexer.helpers.metrics import record_error
from indexer.helpers.identifiers import WorkPublicationStatusIdentifiers
from indexer.helpers.solr import submit_to_solr
from indexer.helpers.utilities import parallelise
from indexer.records.work import (
    create_work_index_documents,
)

log = logging.getLogger("muscat_indexer")


def _get_works(cfg: dict) -> Generator[dict]:
    log.info("Getting list of works to index")
    dbname = "public"

    id_where_clause: str = ""
    if "id" in cfg:
        id_where_clause = f"AND work.id = {cfg['id']}"

    sql_query: str = f"""
SELECT work.id AS id, work.marc_source AS marc_source, peep.id AS person_id,
    work.created_at AS created, work.updated_at AS updated,
    (SELECT jsonb_agg(DISTINCT CONCAT('source_', ss.id))
        FROM {dbname}.sources_to_works AS sw
        LEFT JOIN {dbname}.sources AS ss ON sw.source_id = ss.id
        WHERE sw.work_id = work.id AND ss.wf_stage = 1
    ) AS sources,
    (SELECT jsonb_agg(DISTINCT
                jsonb_build_object('id', (pub.id::text),
                     'author', pub.author,
                     'title', pub.title,
                     'journal', pub.journal,
                     'date', pub.date,
                     'place', pub.place,
                     'short_name', pub.short_name,
                     'marc_source', pub.marc_source,
                     'work_catalogue_status', pub.work_catalogue))
        FROM {dbname}.works_to_publications wpt
        LEFT JOIN {dbname}.publications pub ON wpt.publication_id = pub.id
        WHERE wpt.work_id = work.id AND pub.work_catalogue IN ({WorkPublicationStatusIdentifiers.COMPLETED}, {WorkPublicationStatusIdentifiers.PARTIALLY_COMPLETED}, {WorkPublicationStatusIdentifiers.ALTERNATE})
    ) AS publication_entries,
    jsonb_build_object('name', peep.full_name, 'dates', peep.life_dates) AS person_name,
    (SELECT jsonb_agg(DISTINCT
                jsonb_build_object('id', (pub.id::text),
                     'author', pub.author,
                     'title', pub.title,
                     'journal', pub.journal,
                     'date', pub.date,
                     'place', pub.place,
                     'short_name', pub.short_name,
                     'marc_source', pub.marc_source))
        FROM {dbname}.works_to_publications wpt
        LEFT JOIN {dbname}.publications pub ON wpt.publication_id = pub.id
        WHERE wpt.work_id = work.id AND wpt.marc_tag = '670'
    ) AS source_data_found
FROM {dbname}.works AS work
    LEFT JOIN {dbname}.people peep ON work.person_id = peep.id
    WHERE work.wf_stage = 1 {id_where_clause}
ORDER BY work.id;"""  # noqa: S608

    with postgres_pool.connection() as conn, server_side_cursor(conn, "works") as curs:
            curs.execute(sql_query)
            while rows := curs.fetchmany(cfg["postgres"]["resultsize"]):
                yield rows


def index_works(cfg: dict) -> bool:
    log.info("Indexing works")
    work_groups = _get_works(cfg)
    parallelise(work_groups, index_work_groups, cfg)

    return True


def index_work_groups(works: list, cfg: dict) -> bool:
    log.info("Indexing Work Group")
    records_list: list = []

    for record in works:
        try:
            docs = create_work_index_documents(record, cfg)
        except RequiredFieldException:
            log.critical("Could not index work %s", record["id"])
            record_error(cfg)
            continue

        log.debug("Appending work document")
        records_list.extend(docs)

    check: bool = True if cfg["dry"] else submit_to_solr(records_list, cfg)

    if not check:
        log.error("There was an error submitting works to Solr")

    return check
