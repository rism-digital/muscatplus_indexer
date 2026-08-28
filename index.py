import argparse
import dataclasses
import faulthandler
import logging.config
import multiprocessing
import os.path
import sys
import time
import timeit
import traceback
from collections.abc import Callable
from multiprocessing.managers import SyncManager
from pathlib import Path
from queue import Queue

import sentry_sdk
import yaml
from sentry_sdk.integrations.logging import LoggingIntegration

from cantus_indexer.index import clean_cantus, index_cantus
from cantus_indexer.latest_record import get_latest_cantus_datetime
from diamm_indexer.index import clean_diamm, index_diamm
from diamm_indexer.latest_record import get_latest_diamm_datetime
from indexer.helpers.metrics import (
    calculate_metric_outcome,
    drain_event_errors,
    render_metrics,
    write_metrics_atomically,
)
from indexer.helpers.solr import (
    empty_solr_core,
    get_final_record_counts,
    reload_core,
    submit_to_solr,
    swap_cores,
)
from indexer.index_digital_objects import index_digital_objects
from indexer.index_holdings import index_holdings
from indexer.index_institutions import index_institutions
from indexer.index_inventory_items import index_inventory_items
from indexer.index_liturgical_festivals import index_liturgical_festivals
from indexer.index_people import index_people
from indexer.index_places import index_places
from indexer.index_publications import index_publications
from indexer.index_sources import index_sources
from indexer.index_subjects import index_subjects
from indexer.index_tombstones import index_tombstones
from indexer.index_works import index_works

faulthandler.enable()

log_config: dict = yaml.full_load(open("logging.yml"))  # noqa: SIM115

logging.config.dictConfig(log_config)
log = logging.getLogger("muscat_indexer")


@dataclasses.dataclass
class MetricsSession:
    directory: str
    job_name: str
    manager: SyncManager
    queue: Queue


@dataclasses.dataclass
class MainResult:
    success: bool
    metrics_session: MetricsSession | None
    duration_seconds: float
    final_record_counts: dict[tuple[str, str], int]
    count_errors: int


IndexStep = tuple[str, Callable[[dict], bool]]


def selected_index_steps(
    include: list[str] | None,
    exclude: list[str],
    index_groups: dict[str, tuple[IndexStep, ...]],
) -> list[IndexStep]:
    selected_groups = include or list(index_groups)
    selected_steps: list[IndexStep] = []
    seen_steps: set[tuple[str, Callable[[dict], bool]]] = set()

    for group_name in selected_groups:
        if group_name in exclude or group_name not in index_groups:
            continue

        for step in index_groups[group_name]:
            if step not in seen_steps:
                selected_steps.append(step)
                seen_steps.add(step)

    return selected_steps


def run_index_step(
    cfg: dict,
    metrics_queue: object | None,
    project: str,
    record_type: str,
    fn: Callable[[dict], bool],
) -> bool:
    return fn(metrics_config(cfg, metrics_queue, project, record_type))


def metrics_config(
    cfg: dict,
    metrics_queue: object | None,
    project: str,
    record_type: str,
) -> dict:
    return (
        cfg
        if metrics_queue is None
        else cfg
        | {
            "metrics_context": {
                "queue": metrics_queue,
                "project": project,
                "record_type": record_type,
            }
        }
    )


def initialise_metrics(args: argparse.Namespace, cfg: dict) -> MetricsSession | None:
    if args.dry:
        return None

    metrics_cfg = cfg.get("metrics", {})
    metrics_dir = args.metrics_dir or metrics_cfg.get("directory", "")
    metrics_job_name = args.metrics_job_name or metrics_cfg.get(
        "job_name", "muscatplus_indexer"
    )

    if not metrics_dir:
        return None

    manager = multiprocessing.Manager()
    return MetricsSession(
        directory=metrics_dir,
        job_name=metrics_job_name,
        manager=manager,
        queue=manager.Queue(),
    )


def make_main_result(
    success: bool,
    metrics_session: MetricsSession | None,
    started_at: float,
    final_record_counts: dict[tuple[str, str], int] | None = None,
    count_errors: int = 0,
) -> MainResult:
    return MainResult(
        success=success,
        metrics_session=metrics_session,
        duration_seconds=timeit.default_timer() - started_at,
        final_record_counts=final_record_counts or {},
        count_errors=count_errors,
    )


def format_duration(duration_seconds: float) -> str:
    hours, remainder = divmod(duration_seconds, 60 * 60)
    minutes, seconds = divmod(remainder, 60)
    return f"{hours:02.0f}:{minutes:02.0f}:{seconds:05.2f}"


def index_indexer(
    cfg: dict,
    start: float,
    end: float,
    diamm_latest: str | None,
    cantus_latest: str | None,
) -> bool:
    version: str = cfg["common"]["version"]

    # The 'indexed' and 'id' fields are added automatically by Solr.
    idx_record: dict = {
        "id": "rism-online-index-info",
        "type": "indexer",
        "indexer_version_sni": version,
        "index_start_fp": start,
        "index_end_fp": end,
        "diamm_latest_dt": diamm_latest,
        "cantus_latest_dt": cantus_latest,
    }

    check: bool = submit_to_solr([idx_record], cfg)

    return check


def only_diamm(cfg: dict) -> bool:
    res: bool = True

    if not cfg["dry"]:
        res &= clean_diamm(cfg)

    res &= index_diamm(cfg)
    res &= reload_core(cfg["solr"]["server"], cfg["indexing_core"])

    if cfg["swap_cores"] and not cfg["dry"]:
        res &= swap_cores(
            cfg["solr"]["server"],
            cfg["solr"]["indexing_core"],
            cfg["solr"]["live_core"],
        )

    return res


def only_cantus(cfg: dict) -> bool:
    res: bool = True

    if not cfg["dry"]:
        res &= clean_cantus(cfg)

    res &= index_cantus(cfg)
    res &= reload_core(cfg["solr"]["server"], cfg["indexing_core"])

    return res


def main(args: argparse.Namespace) -> MainResult:
    idx_start: float = timeit.default_timer()

    cfg_filename: str = "./index_config.yml" if not args.config else args.config

    log.info("Using %s as the index configuration file.", cfg_filename)

    if not os.path.exists(cfg_filename):
        log.fatal("Could not find config file %s.", cfg_filename)
        return make_main_result(False, None, idx_start)

    idx_config: dict = yaml.full_load(open(cfg_filename))  # noqa: SIM115
    metrics_session = initialise_metrics(args, idx_config)
    metrics_queue = metrics_session.queue if metrics_session else None

    # Set up sentry logging
    sentry_logging = LoggingIntegration(
        level=logging.ERROR,  # Capture info and above as breadcrumbs
        event_level=logging.ERROR,  # Send errors as events
    )

    version: str = idx_config["common"]["version"]
    release: str = version

    if version.startswith("v"):
        release = version[1:]

    if args.live:
        actual_indexing_core = idx_config["solr"]["live_core"]
        swap_after_indexing = False
    else:
        actual_indexing_core = idx_config["solr"]["indexing_core"]
        swap_after_indexing = args.swap_cores

    # Add a parameter indicating whether this is a dry run to the config.
    idx_config = idx_config | {
        "dry": args.dry,
        "swap_cores": swap_after_indexing,
        "indexing_core": actual_indexing_core,
    }

    debug_mode: bool = idx_config["common"]["debug"]
    if debug_mode is False:
        sentry_sdk.init(
            dsn=idx_config["sentry"]["dsn"],
            environment=idx_config["sentry"]["environment"],
            integrations=[sentry_logging],
            release=f"muscatplus_indexer@{release}",
        )

    # Track the status of the various sub-tasks by &= against a boolean.
    res = True

    if args.only_diamm:
        log.info("Only running the DIAMM indexer.")
        res &= only_diamm(metrics_config(idx_config, metrics_queue, "diamm", "all"))
        # force a core reload to ensure it's up-to-date
        return make_main_result(res, metrics_session, idx_start)

    if args.only_cantus:
        log.info("Only running the Cantus indexer.")
        res &= only_cantus(metrics_config(idx_config, metrics_queue, "cantus", "all"))
        return make_main_result(res, metrics_session, idx_start)

    index_groups: dict[str, tuple[IndexStep, ...]] = {
        "sources": (("sources", index_sources),),
        "people": (("people", index_people),),
        "places": (("places", index_places),),
        "institutions": (("institutions", index_institutions),),
        "holdings": (("holdings", index_holdings),),
        "subjects": (("subjects", index_subjects),),
        "festivals": (("festivals", index_liturgical_festivals),),
        "digital-objects": (("digital-objects", index_digital_objects),),
        "works": (("publications", index_publications), ("works", index_works)),
        "publications": (("publications", index_publications),),
        "inventory-items": (("inventory-items", index_inventory_items),),
        "tombstones": (("tombstones", index_tombstones),),
    }

    if args.empty and not args.dry:
        log.info("Emptying Solr indexing core")
        res &= empty_solr_core(idx_config)

    if args.only_id:
        idx_config = idx_config | {"id": args.only_id}

    for metric_type, fn in selected_index_steps(
        args.include, args.exclude, index_groups
    ):
        res &= run_index_step(idx_config, metrics_queue, "muscat", metric_type, fn)

    if not args.skip_diamm:
        res &= index_diamm(metrics_config(idx_config, metrics_queue, "diamm", "all"))

    if not args.skip_cantus:
        res &= index_cantus(metrics_config(idx_config, metrics_queue, "cantus", "all"))

    log.info("Finished indexing records, cleaning up.")
    idx_end: float = timeit.default_timer()
    # The bookkeeping document is not a record-type indexing result.
    idx_config = {
        key: value for key, value in idx_config.items() if key != "metrics_context"
    }

    # If, so far, all the results have been successful, and we're not in a dry run, then
    # add the final index record and reload the core.
    final_record_counts: dict[tuple[str, str], int] = {}
    count_errors = 0
    if res and not args.dry:
        # Add a single record that records some metadata about this index run
        log.info("Adding indexer record.")
        diamm_datetime: str | None = (
            get_latest_diamm_datetime() if not args.skip_diamm else None
        )

        cantus_datetime: str | None = (
            get_latest_cantus_datetime() if not args.skip_cantus else None
        )
        res &= index_indexer(
            idx_config, idx_start, idx_end, diamm_datetime, cantus_datetime
        )

        # force a core reload to ensure it's up-to-date
        res &= reload_core(idx_config["solr"]["server"], idx_config["indexing_core"])
        if res and metrics_session:
            final_record_counts, count_errors = get_final_record_counts(idx_config)

    # Finally, if all the previous statuses are True, we're supposed to swap the cores, and we're not in a dry run,
    # then consider that indexing was successful and swap the indexer core with the live core.
    if res and idx_config["swap_cores"] and not args.dry:
        log.info("Swapping cores")
        res &= swap_cores(
            idx_config["solr"]["server"],
            idx_config["solr"]["indexing_core"],
            idx_config["solr"]["live_core"],
        )

    if not res:
        log.error("Indexing failed.")

    log.info("Indexing successful.")
    return make_main_result(
        res, metrics_session, idx_start, final_record_counts, count_errors
    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "-v", "--verbose", action="store_true", help="Enable verbose output"
    )
    parser.add_argument(
        "--metrics-dir",
        help="Directory where Prometheus textfile metrics are written. Overrides config.",
    )
    parser.add_argument(
        "--metrics-job-name",
        help="Prometheus metric prefix and output filename. Overrides config.",
    )
    parser.add_argument(
        "-e",
        "--empty",
        dest="empty",
        action="store_true",
        help="Empty the core prior to indexing",
    )
    parser.add_argument(
        "-s",
        "--no-swap",
        dest="swap_cores",
        action="store_false",
        help="Do not swap cores (default is to swap)",
    )
    parser.add_argument(
        "-L",
        "--live",
        dest="live",
        action="store_true",
        help="""Index directly to the live core. Does not swap. When used with empty it will delete
                all records in the live core so it should be used with caution""",
    )

    parser.add_argument(
        "-c",
        "--config",
        dest="config",
        help="Path to an index config file; default is ./index_config.yml.",
    )
    parser.add_argument(
        "-d",
        "--dry-run",
        dest="dry",
        action="store_true",
        help="Perform a dry run; performs all manipulation but does not send the results to Solr.",
    )

    parser.add_argument("--include", action="extend", nargs="*")
    parser.add_argument("--exclude", action="extend", nargs="*", default=[])

    parser.add_argument("--id", dest="only_id", help="Only index a single ID")

    parser.add_argument(
        "--skip-diamm",
        dest="skip_diamm",
        action="store_true",
        help="Skip DIAMM indexing.",
    )
    parser.add_argument(
        "--only-diamm",
        dest="only_diamm",
        action="store_true",
        help="Only index DIAMM into the indexing core. Does not swap afterwards.",
    )

    parser.add_argument(
        "--skip-cantus",
        dest="skip_cantus",
        action="store_true",
        help="Skip Cantus indexing.",
    )
    parser.add_argument(
        "--only-cantus",
        dest="only_cantus",
        action="store_true",
        help="Only index Cantus into the indexing core. Does not swap afterwards.",
    )

    input_args: argparse.Namespace = parser.parse_args()

    if input_args.live:
        log.info("Indexing to the live core!")
        for i in range(3, 0, -1):
            print(
                f"Waiting 3 seconds for Ctrl-C in case this is not correct. {i}",
                end="\r",
                flush=True,
            )
            time.sleep(1)

    if input_args.include:
        input_args.skip_diamm = True
        input_args.skip_cantus = True

    idx_pid = str(os.getpid())
    pid_file: Path = Path("/tmp", "muscatplus_indexer.pid")  # noqa: S108
    if pid_file.exists() and not input_args.dry:
        log.critical("Process is already running. Exiting")
        sys.exit(1)

    if not input_args.dry:
        pid_file.write_text(idx_pid)

    main_result = MainResult(
        success=False,
        metrics_session=None,
        duration_seconds=0,
        final_record_counts={},
        count_errors=0,
    )
    unhandled_errors = 0
    try:
        main_result = main(input_args)
        log.info(
            "Total time to index main: %s",
            format_duration(main_result.duration_seconds),
        )
    except Exception as e:
        log.critical("Main method raised an exception and could not continue: %s", e)
        traceback.print_exc()
        unhandled_errors = 1
    finally:
        session = main_result.metrics_session
        if session:
            try:
                indexing_errors = drain_event_errors(session.queue) + unhandled_errors
                metric_success, errors = calculate_metric_outcome(
                    main_result.success, indexing_errors, main_result.count_errors
                )
                write_metrics_atomically(
                    session.directory,
                    session.job_name,
                    render_metrics(
                        session.job_name,
                        metric_success,
                        int(time.time()),
                        main_result.duration_seconds,
                        errors,
                        main_result.final_record_counts,
                    ),
                )
            except Exception as e:
                log.error("Could not write Prometheus metrics: %s", e)
            finally:
                session.manager.shutdown()

    if not input_args.dry:
        # Remove the PID file
        pid_file.unlink()

    if main_result.success:
        # Exit with status 0 (success).
        faulthandler.disable()
        sys.exit()
    # Exit with an error code.
    sys.exit(1)
