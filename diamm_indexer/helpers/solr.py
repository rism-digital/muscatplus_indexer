import logging
from typing import Callable

from indexer.exceptions import RequiredFieldException
from indexer.helpers.solr import submit_to_solr

log = logging.getLogger("muscat_indexer")


def record_indexer(records: list, converter: Callable, cfg: dict) -> bool:
    idx_records = []

    for record in records:
        try:
            doc = converter(record, cfg)
        except RequiredFieldException:
            log.error("Could not index %s %s", record['type'], record['id'])
            continue

        idx_records.append(doc)

    check: bool
    if cfg["dry"]:
        check = True
    else:
        check = submit_to_solr(idx_records, cfg)

    if not check:
        log.error("There was an error indexing records.")

    return check
