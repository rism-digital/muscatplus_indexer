import argparse
import faulthandler
import logging.config
import os.path
import sys
import timeit

import yaml

from diamm_indexer.index_institutions import index_institutions
from diamm_indexer.index_sources import index_sources
from indexer.helpers.solr import reload_core, empty_diamm_solr_core
from indexer.helpers.utilities import elapsedtime

log_config: dict = yaml.full_load(open('logging.yml', 'r'))

logging.config.dictConfig(log_config)
log = logging.getLogger("muscat_indexer")


@elapsedtime
def main(args: argparse.Namespace) -> bool:
    idx_start: float = timeit.default_timer()
    cfg_filename: str = args.config if args.config else "./index_config.yml"

    if not os.path.exists(cfg_filename):
        log.fatal("Could not find config file %s", cfg_filename)
        return False
    idx_config: dict = yaml.full_load(open(cfg_filename, 'r'))

    res = True
    if args.empty and not args.dry:
        log.info("Emptying Solr indexing core")
        res |= empty_diamm_solr_core(idx_config)

    inc = ["sources", "institutions"]

    for record_type in inc:
        if record_type == "sources":
            res |= index_sources(idx_config)
        elif record_type == "institutions":
            res |= index_institutions(idx_config)

    if res and not args.dry:
        res |= reload_core(idx_config['solr']['server'],
                           idx_config['solr']['diamm_indexing_core'])

    idx_end: float = timeit.default_timer()

    return res


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("-e", "--empty", dest="empty", action="store_true", help="Empty the Solr core before indexing")
    parser.add_argument("-s", "--no-swap", dest="swap_cores", action="store_false", help="Do not swap cores (default is to swap)")
    parser.add_argument("-c", "--config", dest="config", help="Path to an index config file; default is ./index_config.yml.")
    parser.add_argument("-d", "--dry-run", dest="dry", action="store_true", help="Perform a dry run; performs all manipulation but does not send the results to Solr.")

    input_args: argparse.Namespace = parser.parse_args()

    success: bool = main(input_args)
    if success:
        faulthandler.disable()
        sys.exit()

    # exit with an error code
    sys.exit(1)
