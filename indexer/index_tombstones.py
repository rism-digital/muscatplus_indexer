import logging
from collections import deque
from collections.abc import Generator
from typing import Any

from indexer.helpers.db import postgres_pool, server_side_cursor
from indexer.helpers.solr import submit_to_solr
from indexer.helpers.utilities import parallelise
from indexer.records.tombstone import create_tombstone_index_document

log = logging.getLogger("muscat_indexer")


def _get_tombstone_groups(cfg: dict) -> Generator[list[dict[str, Any]]]:

    sql = """SELECT v.item_type AS item_type, v.item_id AS item_id,
                        v.created_at AS deleted,
                        CASE
                            WHEN v.object ~ '(?m)^std_title:' THEN btrim(substring(v.object FROM '(?m)^std_title:[[:space:]]*([^\n]*)'))
                            WHEN v.object ~ '(?m)^source_id:' THEN 'sources/' || btrim(substring(v.object FROM '(?m)^source_id:[[:space:]]*([^\n]*)'))
                            WHEN v.object ~ '(?m)^full_name:' THEN btrim(substring(v.object FROM '(?m)^full_name:[[:space:]]*([^\n]*)'))
                            WHEN v.object ~ '(?m)^title:' THEN btrim(substring(v.object FROM '(?m)^title:[[:space:]]*([^\n]*)'))
                            WHEN v.object ~ '(?m)^name:' THEN btrim(substring(v.object FROM '(?m)^name:[[:space:]]*([^\n]*)'))
                        END AS name
                 FROM versions AS v
                 WHERE v.event = 'destroy'
                   AND v.item_type IN ('Source', 'Person', 'Institution', 'Holding')
                 ORDER BY item_type DESC;"""
    with postgres_pool.connection() as conn, server_side_cursor(conn, "tombstones") as curs:
            curs.execute(sql)
            while rows := curs.fetchmany(cfg["postgres"]["resultsize"]):
                yield rows


def index_tombstones(cfg: dict) -> bool:
    log.info("Indexing Tombstones")
    tombstone_groups = _get_tombstone_groups(cfg)
    parallelise(tombstone_groups, index_tombstone_groups, cfg)

    return True


def index_tombstone_groups(tombstones: list, cfg: dict) -> bool:
    log.info("Indexing Tombstone Group")
    records_to_index: deque = deque()

    for record in tombstones:
        doc: dict[str, object] = create_tombstone_index_document(record, cfg)
        records_to_index.append(doc)

    check: bool = True if cfg["dry"] else submit_to_solr(list(records_to_index), cfg)

    if not check:
        log.error("There was an error submitting tombstones to Solr")

    return check
