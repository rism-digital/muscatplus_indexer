import logging.config
import sys
from typing import Dict
import argparse
import yaml

from indexer.helpers.utilities import elapsedtime
from indexer.helpers.solr import solr_idx_conn, swap_cores,empty_solr_core
from indexer.index_holdings import index_holdings
from indexer.index_institutions import index_institutions
from indexer.index_liturgical_festivals import index_liturgical_festivals
from indexer.index_people import index_people
from indexer.index_places import index_places
from indexer.index_sources import index_sources
from indexer.index_subjects import index_subjects

log_config: Dict = yaml.full_load(open('logging.yml', 'r'))
idx_config: Dict = yaml.full_load(open('index_config.yml', 'r'))

logging.config.dictConfig(log_config)
log = logging.getLogger("muscat_indexer")


@elapsedtime
def main(args) -> bool:
    empt = src = ppl = plc = ins = hld = sub = fst = True

    if args.empty:
        log.info("Emptying Solr indexing core")
        empt = empty_solr_core()

    if args.idx_sources:
        src = index_sources(idx_config)
    if args.idx_people:
        ppl = index_people(idx_config)
    if args.idx_places:
        plc = index_places(idx_config)
    if args.idx_institutions:
        ins = index_institutions(idx_config)
    if args.idx_holdings:
        hld = index_holdings(idx_config)
    if args.idx_subjects:
        sub = index_subjects(idx_config)
    if args.idx_festivals:
        fst = index_liturgical_festivals(idx_config)

    log.info("Performing Solr Commit")
    solr_idx_conn.commit()

    # If all the previous statuses are True, then indexing was successful.
    idx_success: bool = empt and src and ppl and plc and ins and hld and sub and fst

    if idx_success and args.swap_cores:
        swap: bool = swap_cores(idx_config['solr']['server'],
                                idx_config['solr']['indexing_core'],
                                idx_config['solr']['live_core'])
        return swap and idx_success

    # If we are here, then the idx_success is false and something went wrong.
    log.error("Indexing failed.")
    return idx_success


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("-v", "--verbose", action="store_true", help="Enable verbose output")
    parser.add_argument("-e", "--empty", dest="empty", action="store_true", help="Empty the core prior to indexing")
    parser.add_argument("-s", "--no-swap", dest="swap_cores", action="store_false", help="Do not swap cores (default is to swap)")
    parser.add_argument("--no-sources", dest="idx_sources", action="store_false", help="Do not index sources (default is true)")
    parser.add_argument("--no-people", dest="idx_people", action="store_false", help="Do not index people (default is true)")
    parser.add_argument("--no-places", dest="idx_places", action="store_false", help="Do not index places (default is true)")
    parser.add_argument("--no-institutions", dest="idx_institutions", action="store_false", help="Do not index institutions (default is true)")
    parser.add_argument("--no-holdings", dest="idx_holdings", action="store_false", help="Do not index holdings (default is true)")
    parser.add_argument("--no-subjects", dest="idx_subjects", action="store_false", help="Do not index subjects (default is true)")
    parser.add_argument("--no-festivals", dest="idx_festivals", action="store_false", help="Do not index liturgical festivals (default is true)")

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
        sys.exit()
    # Exit with an error code.
    sys.exit(1)
