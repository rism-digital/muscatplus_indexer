import logging

from cmo_indexer.index_sources import index_sources
from indexer.helpers.solr import empty_project_records
from indexer.helpers.utilities import elapsedtime

log = logging.getLogger("muscat_indexer")


@elapsedtime
def index_cmo(idx_config: dict) -> bool:
    log.info("Running CMO Indexer")
    res = True

    inc = ["sources"]

    for record_type in inc:
        if record_type == "sources":
            res &= index_sources(idx_config)

    return res


def clean_cmo(idx_config: dict) -> bool:
    log.info("Cleaning CMO records")
    return empty_project_records("cmo", idx_config)
