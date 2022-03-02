import logging.config
import sys
import argparse
import yaml

from indexer.helpers.utilities import elapsedtime
from indexer.helpers.solr import swap_cores, empty_solr_core, reload_core
from indexer.index_holdings import index_holdings
from indexer.index_institutions import index_institutions
from indexer.index_liturgical_festivals import index_liturgical_festivals
from indexer.index_people import index_people
from indexer.index_places import index_places
from indexer.index_sources import index_sources
from indexer.index_subjects import index_subjects
import faulthandler


faulthandler.enable()

log_config: dict = yaml.full_load(open('logging.yml', 'r'))
idx_config: dict = yaml.full_load(open('index_config.yml', 'r'))

logging.config.dictConfig(log_config)
log = logging.getLogger("muscat_indexer")


@elapsedtime
def main(args) -> bool:
    res = True

    inc: list = []
    if not args.include:
        inc = ["sources", "people", "places", "institutions", "holdings", "subjects", "festivals"]
    else:
        inc = args.include

    if args.empty:
        log.info("Emptying Solr indexing core")
        res |= empty_solr_core()

    if args.only_id:
        idx_config.update({"id": args.only_id})

    for record_type in inc:
        if record_type == "sources" and "sources" not in args.exclude:
            res |= index_sources(idx_config)
        elif record_type == "people" and "people" not in args.exclude:
            res |= index_people(idx_config)
        elif record_type == "places" and "places" not in args.exclude:
            res |= index_places(idx_config)
        elif record_type == "institutions" and "institutions" not in args.exclude:
            res |= index_institutions(idx_config)
        elif record_type == "holdings" and "holdings" not in args.exclude:
            res |= index_holdings(idx_config)
        elif record_type == "subjects" and "subjects" not in args.exclude:
            res |= index_subjects(idx_config)
        elif record_type == "festivals" and "festivals" not in args.exclude:
            res |= index_liturgical_festivals(idx_config)

    log.info("Finished indexing records, cleaning up.")

    # force a core reload to ensure it's up-to-date
    res |= reload_core(idx_config['solr']['server'],
                       idx_config['solr']['indexing_core'])

    # If all the previous statuses are True, then consider that indexing was successful.
    if res and args.swap_cores:
        swap: bool = swap_cores(idx_config['solr']['server'],
                                idx_config['solr']['indexing_core'],
                                idx_config['solr']['live_core'])
        return swap and res

    if not res:
        log.error("Indexing failed.")
    else:
        log.info("Indexing successful.")
    return res


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("-v", "--verbose", action="store_true", help="Enable verbose output")
    parser.add_argument("-e", "--empty", dest="empty", action="store_true", help="Empty the core prior to indexing")
    parser.add_argument("-s", "--no-swap", dest="swap_cores", action="store_false", help="Do not swap cores (default is to swap)")

    parser.add_argument("--include", action="extend", nargs="*")
    parser.add_argument("--exclude", action="extend", nargs="*", default=[])

    parser.add_argument("--id", dest="only_id", help="Only index a single ID")

    interval_group = parser.add_mutually_exclusive_group()
    interval_group.add_argument("-d", "--daily", action="store_true",
                                help="Index all records updated in the last day (24h)")
    interval_group.add_argument("-w", "--weekly", action="store_true",
                                help="Index all records updated in the last week (7 days)")
    interval_group.add_argument("-f", "--full", action="store_false",
                                help="Index all records (default). Empties the core prior to indexing.")
    args = parser.parse_args()

    success: bool = main(args)
    if success:
        # Exit with status 0 (success).
        faulthandler.disable()
        sys.exit()
    # Exit with an error code.
    sys.exit(1)
