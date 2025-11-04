import logging
from collections import deque
from collections.abc import Generator

from indexer.helpers.db import mysql_pool
from indexer.helpers.solr import submit_to_solr
from indexer.helpers.utilities import parallelise
from indexer.records.digital_object import create_digital_object_index_document

log = logging.getLogger("muscat_indexer")


def _get_digital_objects(cfg: dict) -> Generator[dict]:
    conn = mysql_pool.connection()
    curs = conn.cursor()
    dbname: str = cfg["mysql"]["database"]

    sql_query: str = f"""
        SELECT do.id AS dobject_id, s.id AS obj_id, d.object_link_type AS obj_type, s.std_title AS name,
               do.description AS description, do.attachment_content_type AS content_type, do.attachment_file_name AS file_name
            FROM {dbname}.digital_object_links AS d
            JOIN {dbname}.sources AS s ON d.object_link_id = s.id
            LEFT JOIN {dbname}.digital_objects AS do ON d.digital_object_id = do.id
            WHERE d.object_link_type = 'Source' AND do.attachment_file_name IS NOT NULL

        UNION ALL

        SELECT do.id AS dobject_id, p.id AS obj_id, d.object_link_type AS obj_type, CONCAT(p.full_name, COALESCE(CONCAT(' (', p.life_dates, ')'), '')) AS name,
               do.description AS description, do.attachment_content_type AS content_type, do.attachment_file_name AS file_name
            FROM {dbname}.digital_object_links AS d
            JOIN {dbname}.people AS p ON d.object_link_id = p.id
            LEFT JOIN {dbname}.digital_objects AS do ON d.digital_object_id = do.id
            WHERE d.object_link_type = 'Person' AND do.attachment_file_name IS NOT NULL

        UNION ALL

        SELECT do.id AS dobject_id, h.id AS obj_id, d.object_link_type AS obj_type, h.shelf_mark AS name,
               do.description AS description, do.attachment_content_type AS content_type, do.attachment_file_name AS file_name
            FROM {dbname}.digital_object_links AS d
            JOIN {dbname}.holdings AS h ON d.object_link_id = h.id
            LEFT JOIN {dbname}.digital_objects AS do ON d.digital_object_id = do.id
            WHERE d.object_link_type = 'Holding' AND do.attachment_file_name IS NOT NULL

        UNION ALL

        SELECT do.id AS dobject_id, i.id AS obj_id, d.object_link_type AS obj_type, i.full_name AS name,
               do.description AS description, do.attachment_content_type AS content_type, do.attachment_file_name AS file_name
            FROM {dbname}.digital_object_links AS d
            JOIN {dbname}.institutions AS i ON d.object_link_id = i.id
            LEFT JOIN {dbname}.digital_objects AS do ON d.digital_object_id = do.id
        WHERE d.object_link_type = 'Institution' AND do.attachment_file_name IS NOT NULL

        UNION ALL

        SELECT do.id AS dobject_id, w.id AS obj_id, d.object_link_type AS obj_type, w.title AS name,
               do.description AS description, do.attachment_content_type AS content_type, do.attachment_file_name AS file_name
            FROM {dbname}.digital_object_links AS d
            JOIN {dbname}.works AS w ON d.object_link_id = w.id
            LEFT JOIN {dbname}.digital_objects AS do ON d.digital_object_id = do.id
        WHERE d.object_link_type = 'Work' AND do.attachment_file_name IS NOT NULL"""  # noqa: S608

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
    log.info("Indexing Digital Objects")
    records_to_index: deque = deque()

    for record in dobjects:
        doc = create_digital_object_index_document(record, cfg)
        records_to_index.append(doc)

    check: bool = True if cfg["dry"] else submit_to_solr(list(records_to_index), cfg)

    if not check:
        log.error("There was an error submitting digital objects to Solr")

    return check
