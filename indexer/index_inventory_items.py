import logging
from collections import deque
from collections.abc import Generator
from typing import Any

from indexer.helpers.db import postgres_pool, server_side_cursor
from indexer.helpers.solr import submit_to_solr
from indexer.helpers.utilities import parallelise
from indexer.records.inventory_item import create_inventory_item_index_document

log = logging.getLogger("muscat_indexer")


def _get_inventory_items_groups(cfg: dict) -> Generator[list[dict[str, Any]]]:
    record_id = int(cfg["id"]) if "id" in cfg else None
    sql = """
        WITH ranked AS (
            SELECT ii.id, ii.source_id,
                   row_number() OVER (PARTITION BY ii.source_id ORDER BY ii.id) AS zero_row_num
            FROM inventory_items AS ii WHERE ii.source_order = 0
        )
        SELECT ii.id, ii.title, ii.marc_source, ii.source_id,
               coalesce(ranked.zero_row_num, ii.source_order) AS source_order,
               ii.created_at AS created, ii.updated_at AS updated,
               (SELECT jsonb_agg(DISTINCT jsonb_build_object(
                    'relator_code', stos.relator_code, 'marc_source', sours.marc_source))
                FROM inventory_items_to_sources AS stos
                LEFT JOIN sources AS sours ON stos.source_id = sours.id
                WHERE stos.marc_tag = '787' AND stos.inventory_item_id = ii.id) AS related_sources
        FROM inventory_items AS ii LEFT JOIN ranked ON ranked.id = ii.id
        WHERE (%s::bigint IS NULL OR ii.id = %s) ORDER BY ii.source_id, source_order;"""
    with postgres_pool.connection() as conn, server_side_cursor(conn, "inventory_items") as curs:
            curs.execute(sql, (record_id, record_id))
            while rows := curs.fetchmany(cfg["postgres"]["resultsize"]):
                yield rows


def index_inventory_items(cfg: dict) -> bool:
    parallelise(_get_inventory_items_groups(cfg), index_inventory_item_groups, cfg)
    return True


def index_inventory_item_groups(inventory_items: list, cfg: dict) -> bool:
    log.info("Indexing Inventory Items")
    records_to_index: deque = deque()
    for record in inventory_items:
        records_to_index.extend(create_inventory_item_index_document(record, cfg))
    check = True if cfg["dry"] else submit_to_solr(list(records_to_index), cfg)
    if not check:
        log.error("There was an error submitting inventory items to Solr")
    return check
