import logging
from collections import deque
from collections.abc import Generator

from indexer.helpers.db import mysql_pool
from indexer.helpers.solr import submit_to_solr
from indexer.helpers.utilities import parallelise
from indexer.records.tombstone import create_tombstone_index_document

log = logging.getLogger("muscat_indexer")


def _get_tombstone_groups(cfg: dict) -> Generator[dict, None, None]:
    conn = mysql_pool.connection()
    curs = conn.cursor()
    dbname: str = cfg["mysql"]["database"]

    curs.execute(
        f"""SELECT * FROM {dbname}.tombstones;"""  # noqa: S608
    )

    while rows := curs._cursor.fetchmany(cfg["mysql"]["resultsize"]):  # noqa
        yield rows

    curs.close()
    conn.close()


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
