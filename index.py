import argparse
import faulthandler
import logging.config
import os.path
import sys
import timeit
from pathlib import Path

import sentry_sdk
import yaml
from sentry_sdk.integrations.logging import LoggingIntegration

from cantus_indexer.index import clean_cantus, index_cantus
# from cmo_indexer.index import index_cmo, clean_cmo
from diamm_indexer.index import index_diamm, clean_diamm
from indexer.helpers.db import run_preflight_queries
from indexer.helpers.solr import swap_cores, empty_solr_core, reload_core, submit_to_solr
from indexer.helpers.utilities import elapsedtime
from indexer.index_digital_objects import index_digital_objects
from indexer.index_holdings import index_holdings
from indexer.index_institutions import index_institutions
from indexer.index_liturgical_festivals import index_liturgical_festivals
from indexer.index_people import index_people
from indexer.index_places import index_places
from indexer.index_sources import index_sources
from indexer.index_subjects import index_subjects
from indexer.index_works import index_works

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


def only_diamm(cfg: dict) -> bool:
    res: bool = True

    if not cfg["dry"]:
        res &= clean_diamm(cfg)

    res &= index_diamm(cfg)
    res &= reload_core(cfg['solr']['server'],
                       cfg['solr']['indexing_core'])

    if cfg["swap_cores"] and not cfg["dry"]:
        res &= swap_cores(cfg['solr']['server'],
                          cfg['solr']['indexing_core'],
                          cfg['solr']['live_core'])

    return res


def only_cantus(cfg: dict) -> bool:
    res: bool = True

    if not cfg["dry"]:
        res &= clean_cantus(cfg)

    res &= index_cantus(cfg)
    res &= reload_core(cfg['solr']['server'],
                       cfg['solr']['indexing_core'])

    # if cfg["swap_cores"] and not cfg["dry"]:
    #     res &= swap_cores(cfg['solr']['server'],
    #                       cfg['solr']['indexing_core'],
    #                       cfg['solr']['live_core'])

    return res


# def only_cmo(cfg: dict) -> bool:
#     res: bool = True
#     if not cfg["dry"]:
#         res &= clean_cmo(cfg)
#
#     res &= index_cmo(cfg)
#     res &= reload_core(cfg['solr']['server'],
#                        cfg['solr']['indexing_core'])
#
#     if cfg["swap_cores"] and not cfg["dry"]:
#         res &= swap_cores(cfg['solr']['server'],
#                           cfg['solr']['indexing_core'],
#                           cfg['solr']['live_core'])
#
#     return res
#

@elapsedtime
def main(args: argparse.Namespace) -> bool:
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
        level=logging.ERROR,  # Capture info and above as breadcrumbs
        event_level=logging.ERROR  # Send errors as events
    )

    version: str = idx_config["common"]["version"]
    release: str = version

    if version.startswith("v"):
        release = version[1:]

    # Add a parameter indicating whether this is a dry run to the config.
    idx_config.update({
        "dry": args.dry,
        "swap_cores": args.swap_cores
    })

    debug_mode: bool = idx_config["common"]["debug"]
    if debug_mode is False:
        sentry_sdk.init(
            dsn=idx_config["sentry"]["dsn"],
            environment=idx_config["sentry"]["environment"],
            integrations=[sentry_logging],
            release=f"muscatplus_indexer@{release}"
        )

    # Track the status of the various sub-tasks by &= against a boolean.
    res = True

    if args.only_diamm:
        log.info("Only running the DIAMM indexer.")
        res &= only_diamm(idx_config)
        # force a core reload to ensure it's up-to-date
        return res

    if args.only_cantus:
        log.info("Only running the Cantus indexer.")
        res &= only_cantus(idx_config)
        return res

    # if args.only_cmo:
    #     log.info("Only running the CMO indexer.")
    #     res &= only_cmo(idx_config)
    #     return res

    inc: list
    if not args.include:
        inc = ["sources",
               "people",
               "places",
               "institutions",
               "holdings",
               "subjects",
               "festivals",
               "digital-objects",
               "works"]
    else:
        inc = args.include

    if args.empty and not args.dry:
        log.info("Emptying Solr indexing core")
        res &= empty_solr_core(idx_config)

    if args.only_id:
        idx_config.update({"id": args.only_id})

    if not args.dry:
        res &= run_preflight_queries(idx_config)

    for record_type in inc:
        if record_type == "sources" and "sources" not in args.exclude:
            res &= index_sources(idx_config)
        elif record_type == "people" and "people" not in args.exclude:
            res &= index_people(idx_config)
        elif record_type == "places" and "places" not in args.exclude:
            res &= index_places(idx_config)
        elif record_type == "institutions" and "institutions" not in args.exclude:
            res &= index_institutions(idx_config)
        elif record_type == "holdings" and "holdings" not in args.exclude:
            res &= index_holdings(idx_config)
        elif record_type == "subjects" and "subjects" not in args.exclude:
            res &= index_subjects(idx_config)
        elif record_type == "festivals" and "festivals" not in args.exclude:
            res &= index_liturgical_festivals(idx_config)
        elif record_type == "digital-objects" and "digital-objects" not in args.exclude:
            res &= index_digital_objects(idx_config)
        elif record_type == "works" and "works" not in args.exclude:
            res &= index_works(idx_config)

    if not args.skip_diamm:
        res &= index_diamm(idx_config)

    if not args.skip_cantus:
        res &= index_cantus(idx_config)

    # if not args.skip_cmo:
    #     res &= index_cmo(idx_config)

    log.info("Finished indexing records, cleaning up.")
    idx_end: float = timeit.default_timer()

    # If, so far, all the results have been successful and we're not in a dry run, then
    # add the final index record and reload the core.
    if res and not args.dry:
        # Add a single record that records some metadata about this index run
        log.info("Adding indexer record.")
        res &= index_indexer(idx_config, idx_start, idx_end)

        # force a core reload to ensure it's up-to-date
        res &= reload_core(idx_config['solr']['server'],
                           idx_config['solr']['indexing_core'])

    # Finally, if all the previous statuses are True, we're supposed to swap the cores, and we're not in a dry run,
    # then consider that indexing was successful and swap the indexer core with the live core.
    if res and args.swap_cores and not args.dry:
        res &= swap_cores(idx_config['solr']['server'],
                          idx_config['solr']['indexing_core'],
                          idx_config['solr']['live_core'])

    if not res:
        log.error("Indexing failed.")
    else:
        log.info("Indexing successful.")

    return res


if __name__ == "__main__":
    idx_pid = str(os.getpid())
    pid_file: Path = Path("/tmp", "muscatplus_indexer.pid")
    if pid_file.exists():
        log.critical("Process is already running. Exiting")
        sys.exit(1)

    pid_file.write_text(idx_pid)

    parser = argparse.ArgumentParser()
    parser.add_argument("-v", "--verbose", action="store_true", help="Enable verbose output")
    parser.add_argument("-e", "--empty", dest="empty", action="store_true", help="Empty the core prior to indexing")
    parser.add_argument("-s", "--no-swap", dest="swap_cores", action="store_false",
                        help="Do not swap cores (default is to swap)")
    parser.add_argument("-c", "--config", dest="config",
                        help="Path to an index config file; default is ./index_config.yml.")
    parser.add_argument("-d", "--dry-run", dest="dry", action="store_true",
                        help="Perform a dry run; performs all manipulation but does not send the results to Solr.")

    parser.add_argument("--include", action="extend", nargs="*")
    parser.add_argument("--exclude", action="extend", nargs="*", default=[])

    parser.add_argument("--id", dest="only_id", help="Only index a single ID")

    parser.add_argument("--skip-diamm", dest="skip_diamm", action="store_true", help="Skip DIAMM indexing.")
    parser.add_argument("--only-diamm", dest="only_diamm", action="store_true",
                        help="Only index DIAMM into the indexing core. Does not swap afterwards.")

    parser.add_argument("--skip-cantus", dest="skip_cantus", action="store_true", help="Skip Cantus indexing.")
    parser.add_argument("--only-cantus", dest="only_cantus", action="store_true",
                        help="Only index Cantus into the indexing core. Does not swap afterwards.")

    # parser.add_argument("--skip-cmo", dest="skip_cmo", action="store_true", help="Skip CMO indexing.")
    # parser.add_argument("--only-cmo", dest="only_cmo", action="store_true",
    #                     help="Only index CMO into the indexing core. Does not swap afterwards.")

    input_args: argparse.Namespace = parser.parse_args()

    try:
        success: bool = main(input_args)
    except Exception as e:
        log.critical("Main method raised an exception and could not continue: %s", e)
        success = False

    # Remove the PID file
    pid_file.unlink()

    if success:
        # Exit with status 0 (success).
        faulthandler.disable()
        sys.exit()
    # Exit with an error code.
    sys.exit(1)
