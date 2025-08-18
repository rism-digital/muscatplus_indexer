import logging
from collections import deque
from collections.abc import Generator

from indexer.helpers.db import mysql_pool
from indexer.helpers.solr import submit_to_solr
from indexer.helpers.utilities import parallelise
from indexer.records.tombstone import create_tombstone_index_document

log = logging.getLogger("muscat_indexer")


def _get_tombstone_groups(cfg: dict) -> Generator[dict]:
    conn = mysql_pool.connection()
    curs = conn.cursor()
    dbname: str = cfg["mysql"]["database"]

    curs.execute(
        f"""SELECT v.item_type AS item_type, v.item_id AS item_id,
                        v.created_at AS deleted,
                        (SELECT TRIM(
                            CASE
                                WHEN v.object LIKE '%std_title:%' THEN
                                    SUBSTRING_INDEX(
                                            SUBSTRING_INDEX(v.object, 'std_title:', -1),
                                            '\n',
                                            1
                                    )
                                WHEN v.object LIKE '%source_id:%' THEN
                                    SUBSTRING_INDEX(
                                            CONCAT('sources/', SUBSTRING_INDEX(v.object, 'source_id: ', -1)),
                                            '\n',
                                            1
                                    )
                                WHEN v.object LIKE '%full_name:%' THEN
                                    SUBSTRING_INDEX(
                                            SUBSTRING_INDEX(v.object, 'full_name:', -1),
                                            '\n',
                                            1
                                    )
                                WHEN v.object LIKE '%title:%' THEN
                                    SUBSTRING_INDEX(
                                            SUBSTRING_INDEX(v.object, 'title:', -1),
                                            '\n',
                                            1
                                    )

                                WHEN v.object LIKE '%name:%' THEN
                                    SUBSTRING_INDEX(
                                            SUBSTRING_INDEX(v.object, 'name:', -1),
                                            '\n',
                                            1
                                    )
                                ELSE NULL
                            END)) AS name,
                            (TRIM(
                                BOTH '"' FROM SUBSTRING_INDEX(SUBSTRING_INDEX(v.object, 'marc_source: ', -1),
                                '\n',
                                1))) AS marc_source
                 FROM {dbname}.versions AS v
                 WHERE v.event = 'destroy'
                   AND v.item_type IN ('Source', 'Person', 'Institution', 'Holding')
                 ORDER BY item_type DESC"""  # noqa: S608
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
