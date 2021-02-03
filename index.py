import logging.config
import sys
from typing import Dict

import yaml

from indexer.helpers.utilities import elapsedtime
from indexer.helpers.solr import solr_idx_conn, swap_cores
from indexer.index_holdings import index_holdings
from indexer.index_institutions import index_institutions
from indexer.index_people import index_people
from indexer.index_places import index_places
from indexer.index_sources import index_sources
from indexer.index_subjects import index_subjects

log_config: Dict = yaml.full_load(open('logging.yml', 'r'))
idx_config: Dict = yaml.full_load(open('index_config.yml', 'r'))

logging.config.dictConfig(log_config)
log = logging.getLogger("muscat_indexer")


@elapsedtime
def main() -> bool:
    # _ = index_holdings(idx_config)
    # solr_idx_conn.commit()
    # swap: bool = swap_cores(idx_config['solr']['server'],
    #                         idx_config['solr']['indexing_core'],
    #                         idx_config['solr']['live_core'])
    # return True

    src: bool = index_sources(idx_config)
    ppl: bool = index_people(idx_config)
    plc: bool = index_places(idx_config)
    ins: bool = index_institutions(idx_config)
    hld: bool = index_holdings(idx_config)
    sub: bool = index_subjects(idx_config)

    log.info("Performing Solr Commit")
    solr_idx_conn.commit()

    idx_result: bool = src and ppl and plc and ins and hld and sub

    if idx_result:
        swap: bool = swap_cores(idx_config['solr']['server'],
                                idx_config['solr']['indexing_core'],
                                idx_config['solr']['live_core'])
        return swap and idx_result

    # If we are here, then the idx_result is false and something went wrong.
    log.error("Indexing failed.")
    return idx_result


if __name__ == "__main__":
    success: bool = main()
    if success:
        # Exit with status 0 (success).
        sys.exit()
    # Exit with an error code.
    sys.exit(1)
