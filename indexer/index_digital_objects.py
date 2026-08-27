import logging
from collections import deque
from collections.abc import Generator
from typing import Any

from indexer.helpers.db import postgres_pool, server_side_cursor
from indexer.helpers.solr import submit_to_solr
from indexer.helpers.utilities import parallelise
from indexer.records.digital_object import create_digital_object_index_document

log = logging.getLogger("muscat_indexer")


def _get_digital_objects(cfg: dict) -> Generator[list[dict[str, Any]]]:

    sql_query = """
        SELECT dol.id AS dobject_id, s.id AS obj_id, d.object_link_type AS obj_type, s.std_title AS name,
               dol.description AS description, dol.attachment_content_type AS content_type, dol.attachment_file_name AS file_name
            FROM digital_object_links AS d
            JOIN sources AS s ON d.object_link_id = s.id
            LEFT JOIN digital_objects AS dol ON d.digital_object_id = dol.id
            WHERE d.object_link_type = 'Source' AND dol.attachment_file_name IS NOT NULL

        UNION ALL

        SELECT dol.id AS dobject_id, p.id AS obj_id, d.object_link_type AS obj_type, CONCAT(p.full_name, COALESCE(CONCAT(' (', p.life_dates, ')'), '')) AS name,
               dol.description AS description, dol.attachment_content_type AS content_type, dol.attachment_file_name AS file_name
            FROM digital_object_links AS d
            JOIN people AS p ON d.object_link_id = p.id
            LEFT JOIN digital_objects AS dol ON d.digital_object_id = dol.id
            WHERE d.object_link_type = 'Person' AND dol.attachment_file_name IS NOT NULL

        UNION ALL

        SELECT dol.id AS dobject_id, h.id AS obj_id, d.object_link_type AS obj_type, h.shelf_mark AS name,
               dol.description AS description, dol.attachment_content_type AS content_type, dol.attachment_file_name AS file_name
            FROM digital_object_links AS d
            JOIN holdings AS h ON d.object_link_id = h.id
            LEFT JOIN digital_objects AS dol ON d.digital_object_id = dol.id
            WHERE d.object_link_type = 'Holding' AND dol.attachment_file_name IS NOT NULL

        UNION ALL

        SELECT dol.id AS dobject_id, i.id AS obj_id, d.object_link_type AS obj_type, i.full_name AS name,
               dol.description AS description, dol.attachment_content_type AS content_type, dol.attachment_file_name AS file_name
            FROM digital_object_links AS d
            JOIN institutions AS i ON d.object_link_id = i.id
            LEFT JOIN digital_objects AS dol ON d.digital_object_id = dol.id
        WHERE d.object_link_type = 'Institution' AND dol.attachment_file_name IS NOT NULL

        UNION ALL

        SELECT dol.id AS dobject_id, w.id AS obj_id, d.object_link_type AS obj_type, w.title AS name,
               dol.description AS description, dol.attachment_content_type AS content_type, dol.attachment_file_name AS file_name
            FROM digital_object_links AS d
            JOIN works AS w ON d.object_link_id = w.id
            LEFT JOIN digital_objects AS dol ON d.digital_object_id = dol.id
        WHERE d.object_link_type = 'Work' AND dol.attachment_file_name IS NOT NULL;"""

    with postgres_pool.connection() as conn, server_side_cursor(conn, "digital_objects") as curs:
            curs.execute(sql_query)
            while rows := curs.fetchmany(cfg["postgres"]["resultsize"]):
                yield rows


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
