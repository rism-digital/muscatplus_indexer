import logging
from collections import deque
from typing import Generator

from indexer.helpers.db import mysql_pool
from indexer.helpers.solr import submit_to_solr
from indexer.helpers.utilities import parallelise
from indexer.records.digital_object import create_digital_object_index_document

log = logging.getLogger("muscat_indexer")


def _get_digital_objects(cfg: dict) -> Generator[dict, None, None]:
    log.info("Getting list of digital objects to index")
    conn = mysql_pool.connection()
    curs = conn.cursor()
    dbname: str = cfg["mysql"]["database"]

    id_where_clause: str = ""
    if "id" in cfg:
        id_where_clause = f"WHERE dol.digital_object_id = {cfg['id']}"

    sql_query: str = f"""SELECT dol.digital_object_id, dol.object_link_id,
       do.description, do.attachment_content_type,
       do.attachment_file_name, dol.object_link_type
       FROM {dbname}.digital_object_links AS dol
       LEFT JOIN {dbname}.digital_objects AS do ON do.id = dol.digital_object_id
       {id_where_clause};"""

    curs.execute(sql_query)
    while rows := curs._cursor.fetchmany(cfg["mysql"]["resultsize"]):  # noqa
        yield rows

    curs.close()
    conn.close()


def index_digital_objects(cfg: dict) -> bool:
    do_groups = _get_digital_objects(cfg)
    parallelise(do_groups, index_dobject_groups, cfg)

    return True


def index_dobject_groups(dobjects: list, cfg: dict) -> bool:
    records_to_index: deque = deque()

    for record in dobjects:
        doc = create_digital_object_index_document(record, cfg)
        records_to_index.append(doc)

    if cfg["dry"]:
        check = True
    else:
        check = submit_to_solr(list(records_to_index), cfg)

    if not check:
        log.error("There was an error submitting digital objects to Solr")

    return check
