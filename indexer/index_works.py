import logging
from collections.abc import Generator

from indexer.exceptions import RequiredFieldException
from indexer.helpers.db import mysql_pool
from indexer.helpers.solr import submit_to_solr
from indexer.helpers.utilities import parallelise
from indexer.records.work import (
    create_work_index_documents,
)

log = logging.getLogger("muscat_indexer")


def _get_works(cfg: dict) -> Generator[dict]:
    log.info("Getting list of works to index")
    conn = mysql_pool.connection()
    curs = conn.cursor()
    dbname: str = cfg["mysql"]["database"]

    id_where_clause: str = ""
    if "id" in cfg:
        id_where_clause = f"AND work.id = {cfg['id']}"

    sql_query: str = f"""
SELECT work.id AS id, work.marc_source AS marc_source, peep.id AS person_id,
    work.created_at AS created, work.updated_at AS updated,
    (SELECT JSON_ARRAYAGG(DISTINCT
                JSON_OBJECT('id', CONCAT('source_', ss.id),
                            'marc_source', ss.marc_source))
        FROM {dbname}.sources_to_works AS sw
        LEFT JOIN {dbname}.sources AS ss ON sw.source_id = ss.id
        WHERE sw.work_id = work.id AND ss.wf_stage = 1
    ) AS sources,
    (SELECT JSON_ARRAYAGG(DISTINCT
                JSON_OBJECT('id', (CAST(pub.id AS CHAR)),
                     'author', pub.author,
                     'title', pub.title,
                     'journal', pub.journal,
                     'date', pub.date,
                     'place', pub.place,
                     'short_name', pub.short_name,
                     'marc_source', pub.marc_source))
        FROM {dbname}.works_to_publications wpt
        LEFT JOIN {dbname}.publications pub ON wpt.publication_id = pub.id
        WHERE wpt.work_id = work.id
    ) AS publication_entries,
    JSON_OBJECT('name', peep.full_name, 'dates', peep.life_dates) AS person_name
FROM {dbname}.works AS work
    LEFT JOIN {dbname}.people peep ON work.person_id = peep.id
    WHERE work.wf_stage = 1 {id_where_clause}
GROUP BY work.id
ORDER BY work.id;"""  # noqa: S608

    curs.execute(sql_query)

    while rows := curs._cursor.fetchmany(cfg["mysql"]["resultsize"]):
        yield rows

    curs.close()
    conn.close()


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
            continue

        log.debug("Appending work document")
        records_list.extend(docs)

    check: bool = True if cfg["dry"] else submit_to_solr(records_list, cfg)

    if not check:
        log.error("There was an error submitting works to Solr")

    return check
