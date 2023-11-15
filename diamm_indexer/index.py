import logging.config

from diamm_indexer.index_institutions import index_institutions
from diamm_indexer.index_people import index_people
from diamm_indexer.index_sources import index_sources
from indexer.helpers.solr import empty_diamm_records
from indexer.helpers.utilities import elapsedtime

log = logging.getLogger("muscat_indexer")


@elapsedtime
def index_diamm(idx_config: dict) -> bool:
    log.info("Running DIAMM Indexer")
    res = True

    inc = ["sources", "institutions", "people"]

    for record_type in inc:
        if record_type == "sources":
            res &= index_sources(idx_config)
        elif record_type == "institutions":
            res &= index_institutions(idx_config)
        elif record_type == "people":
            res &= index_people(idx_config)
    return res


def clean_diamm(idx_config: dict) -> bool:
    log.info("Cleaning out the old DIAMM records")
    return empty_diamm_records(idx_config)
