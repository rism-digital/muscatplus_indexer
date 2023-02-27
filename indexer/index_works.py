import logging
from collections import deque
from typing import Generator

from indexer.exceptions import RequiredFieldException
from indexer.helpers.db import mysql_pool
from indexer.helpers.solr import submit_to_solr
from indexer.helpers.utilities import parallelise
from indexer.records.work import create_work_index_documents

log = logging.getLogger("muscat_indexer")


def _get_works(cfg: dict) -> Generator[dict, None, None]:
    log.info("Getting list of works to index")
    conn = mysql_pool.connection()
    curs = conn.cursor()
    dbname: str = cfg["mysql"]["database"]

    sql_query: str = f"""SELECT work.id, work.marc_source
    FROM {dbname}.works AS work
    ORDER BY work.id asc;
    """

    curs.execute(sql_query)

    while rows := curs._cursor.fetchmany(cfg['mysql']['resultsize']):
        yield rows

    curs.close()
    conn.close()


def index_works(cfg: dict) -> bool:
    log.info("Indexing works")
    work_groups = _get_works(cfg)
    parallelise(work_groups, index_work_groups, cfg)

    return True


def index_work_groups(works: list, cfg: dict) -> bool:
    log.info("indexing Work Group")
    records_to_index = deque()

    for record in works:
        try:
            docs = create_work_index_documents(record, cfg)
        except RequiredFieldException:
            log.critical("Could not index work %s", record["id"])
            continue

        log.debug("Appending work document")
        records_to_index.extend(docs)

    records_list: list = list(records_to_index)

    if cfg["dry"]:
        check = True
    else:
        check = submit_to_solr(records_list, cfg)

    if not check:
        log.error("There was an error submitting works to Solr")

    return check
