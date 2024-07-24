import logging.config

from cantus_indexer.index_institutions import index_institutions
from cantus_indexer.index_sources import index_sources
from indexer.helpers.solr import empty_project_records
from indexer.helpers.utilities import elapsedtime

log = logging.getLogger("muscat_indexer")


@elapsedtime
def index_cantus(idx_config: dict) -> bool:
    log.info("Running Cantus Indexer")
    res = True

    inc = ["sources", "institutions"]

    for record_type in inc:
        if record_type == "sources":
            res &= index_sources(idx_config)
        elif record_type == "institutions":
            res &= index_institutions(idx_config)
        # elif record_type == "institutions":
        #     res &= index_institutions(idx_config)
    return res


def clean_cantus(idx_config: dict) -> bool:
    log.info("Cleaning out the old Cantus records")
    return empty_project_records("cantus", idx_config)
