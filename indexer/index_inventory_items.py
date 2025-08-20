import logging
from collections import deque
from collections.abc import Generator

from indexer.helpers.db import mysql_pool
from indexer.helpers.solr import submit_to_solr
from indexer.helpers.utilities import parallelise
from indexer.records.inventory_item import create_inventory_item_index_document

log = logging.getLogger("muscat_indexer")


def _get_inventory_items_groups(cfg: dict) -> Generator[dict]:
    conn = mysql_pool.connection()
    curs = conn.cursor()
    dbname: str = cfg["mysql"]["database"]

    sql_statement = f"""
        # noinspection SqlSignature @ routine/"JSON_ARRAYAGG"
        SELECT ii.id AS id, ii.title AS title, ii.marc_source AS marc_source, ii.source_id AS source_id,
            ii.created_at AS created, ii.updated_at AS updated,
            (SELECT JSON_ARRAYAGG(DISTINCT
                             JSON_OBJECT('relator_code', stos.relator_code,
                                         'marc_source', sours.marc_source))
                FROM {dbname}.inventory_items_to_sources AS stos
                LEFT JOIN {dbname}.sources AS sours ON stos.source_id = sours.id
                WHERE marc_tag = '787' AND inventory_item_id = ii.id
            ) AS related_sources
        FROM {dbname}.inventory_items AS ii"""  # noqa: S608

    curs.execute(sql_statement)

    while rows := curs._cursor.fetchmany(size=cfg["mysql"]["resultsize"]):
        yield rows

    curs.close()
    conn.close()


def index_inventory_items(cfg: dict) -> bool:
    items_groups = _get_inventory_items_groups(cfg)
    parallelise(items_groups, index_inventory_item_groups, cfg)

    return True


def index_inventory_item_groups(inventory_items: list, cfg: dict) -> bool:
    log.info("Indexing Inventory Items")
    records_to_index: deque = deque()

    for record in inventory_items:
        doc = create_inventory_item_index_document(record, cfg)
        records_to_index.extend(doc)

    check: bool = True if cfg["dry"] else submit_to_solr(list(records_to_index), cfg)
    if not check:
        log.error("There was an error submitting inventory items to Solr")

    return check
