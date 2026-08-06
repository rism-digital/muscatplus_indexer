import logging.config

from cantus_indexer.index_institutions import index_institutions
from cantus_indexer.index_sources import index_sources
from indexer.helpers.solr import empty_project_records

log = logging.getLogger("muscat_indexer")


def run_project_step(idx_config: dict, record_type: str, fn) -> bool:
    context = idx_config.get("metrics_context")
    step_cfg = (
        idx_config
        if not context
        else idx_config
        | {
            "metrics_context": {
                **context,
                "project": "cantus",
                "record_type": record_type,
            }
        }
    )
    return fn(step_cfg)


def index_cantus(idx_config: dict) -> bool:
    log.info("Running Cantus Indexer")
    res = True

    inc = ["sources", "institutions"]

    for record_type in inc:
        if record_type == "sources":
            res &= run_project_step(idx_config, "sources", index_sources)
        elif record_type == "institutions":
            res &= run_project_step(idx_config, "institutions", index_institutions)
        # elif record_type == "institutions":
        #     res &= index_institutions(idx_config)
    return res


def clean_cantus(idx_config: dict) -> bool:
    log.info("Cleaning out the old Cantus records")
    return empty_project_records("cantus", idx_config)
