import argparse
import faulthandler
import logging.config
import os.path
import sys
import timeit

import sentry_sdk
from sentry_sdk.integrations.logging import LoggingIntegration
import yaml

from indexer.helpers.solr import swap_cores, empty_solr_core, reload_core, submit_to_solr
from indexer.helpers.utilities import elapsedtime
from indexer.index_holdings import index_holdings
from indexer.index_institutions import index_institutions
from indexer.index_liturgical_festivals import index_liturgical_festivals
from indexer.index_people import index_people
from indexer.index_places import index_places
from indexer.index_sources import index_sources
from indexer.index_subjects import index_subjects

faulthandler.enable()

log_config: dict = yaml.full_load(open('logging.yml', 'r'))

logging.config.dictConfig(log_config)
log = logging.getLogger("muscat_indexer")


def index_indexer(cfg: dict, start: float, end: float) -> bool:
    version: str = cfg["common"]["version"]

    # The 'indexed' and 'id' fields are added automatically by Solr.
    idx_record: dict = {
        "type": "indexer",
        "indexer_version_sni": version,
        "index_start_fp": start,
        "index_end_fp": end,
    }

    check: bool = submit_to_solr([idx_record], cfg)

    return check


@elapsedtime
def main(args) -> bool:
    idx_start: float = timeit.default_timer()

    cfg_filename: str

    if not args.config:
        cfg_filename = "./index_config.yml"
    else:
        cfg_filename = args.config

    log.info("Using %s as the index configuration file.", cfg_filename)

    if not os.path.exists(cfg_filename):
        log.fatal("Could not find config file %s.", cfg_filename)
        return False
    idx_config: dict = yaml.full_load(open(cfg_filename, 'r'))

    # Set up sentry logging
    sentry_logging = LoggingIntegration(
        level=logging.ERROR,        # Capture info and above as breadcrumbs
        event_level=logging.ERROR   # Send errors as events
    )

    version: str = idx_config["common"]["version"]
    release: str = version

    if version.startswith("v"):
        release = version[1:]

    debug_mode: bool = idx_config["common"]["debug"]
    if debug_mode is False:
        sentry_sdk.init(
            dsn=idx_config["sentry"]["dsn"],
            environment=idx_config["sentry"]["environment"],
            integrations=[sentry_logging],
            release=f"muscatplus_indexer@{release}"
        )

    res = True

    inc: list
    if not args.include:
        inc = ["sources", "people", "places", "institutions", "holdings", "subjects", "festivals"]
    else:
        inc = args.include

    if args.empty and not args.dry:
        log.info("Emptying Solr indexing core")
        res |= empty_solr_core(idx_config)

    if args.only_id:
        idx_config.update({"id": args.only_id})

    # Add a parameter indicating whether this is a dry run to the config.
    idx_config.update({"dry": args.dry})

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
    idx_end: float = timeit.default_timer()

    # If, so far, all the results have been successful and we're not in a dry run, then
    # add the final index record and reload the core.
    if res and not args.dry:
        # Add a single record that records some metadata about this index run
        log.info("Adding indexer record.")
        res |= index_indexer(idx_config, idx_start, idx_end)

        # force a core reload to ensure it's up-to-date
        res |= reload_core(idx_config['solr']['server'],
                           idx_config['solr']['indexing_core'])

    # Finally, if all the previous statuses are True, we're supposed to swap the cores, and we're not in a dry run,
    # then consider that indexing was successful and swap the indexer core with the live core.
    if res and args.swap_cores and not args.dry:
        swap: bool = swap_cores(idx_config['solr']['server'],
                                idx_config['solr']['indexing_core'],
                                idx_config['solr']['live_core'])
        res |= swap

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
    parser.add_argument("-c", "--config", dest="config", help="Path to an index config file; default is ./index_config.yml.")
    parser.add_argument("-d", "--dry-run", dest="dry", action="store_true", help="Perform a dry run; performs all manipulation but does not send the results to Solr.")

    parser.add_argument("--include", action="extend", nargs="*")
    parser.add_argument("--exclude", action="extend", nargs="*", default=[])

    parser.add_argument("--id", dest="only_id", help="Only index a single ID")

    args = parser.parse_args()

    success: bool = main(args)
    if success:
        # Exit with status 0 (success).
        faulthandler.disable()
        sys.exit()
    # Exit with an error code.
    sys.exit(1)
