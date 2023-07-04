import logging.config

from diamm_indexer.index_institutions import index_institutions
from diamm_indexer.index_people import index_people
from diamm_indexer.index_sources import index_sources
from indexer.helpers.solr import reload_core, empty_diamm_solr_core, swap_cores
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


# if __name__ == "__main__":
#     parser = argparse.ArgumentParser()
#     parser.add_argument("-e", "--empty", dest="empty", action="store_true", help="Empty the Solr core before indexing")
#     parser.add_argument("-s", "--no-swap", dest="swap_cores", action="store_false", help="Do not swap cores (default is to swap)")
#     parser.add_argument("-c", "--config", dest="config", help="Path to an index config file; default is ./index_config.yml.")
#     parser.add_argument("-d", "--dry-run", dest="dry", action="store_true", help="Perform a dry run; performs all manipulation but does not send the results to Solr.")
#
#     input_args: argparse.Namespace = parser.parse_args()
#
#     success: bool = main(input_args)
#     if success:
#         faulthandler.disable()
#         sys.exit()
#
#     # exit with an error code
#     sys.exit(1)
