import logging
from typing import Callable

import httpx
import orjson

from indexer.exceptions import RequiredFieldException

log = logging.getLogger("muscat_indexer")


def empty_solr_core(cfg: dict) -> bool:
    idx_core = cfg["solr"]["indexing_core"]
    return _empty_solr_core(cfg, idx_core)


def _empty_solr_core(cfg: dict, core: str) -> bool:
    solr_address = cfg["solr"]["server"]
    solr_idx_server: str = f"{solr_address}/{core}"

    res = httpx.post(
        f"{solr_idx_server}/update?commit=true",
        content=orjson.dumps({"delete": {"query": "*:*"}}),
        headers={"Content-Type": "application/json"},
        timeout=None,  # noqa: S113
        verify=False,  # noqa: S501
    )

    if 200 <= res.status_code < 400:
        log.debug("Deletion was successful")
        return True
    return False


def empty_project_records(project_identifier: str, cfg: dict) -> bool:
    solr_address = cfg["solr"]["server"]
    idx_core = cfg["solr"]["indexing_core"]
    solr_idx_server: str = f"{solr_address}/{idx_core}"

    res = httpx.post(
        f"{solr_idx_server}/update?commit=true",
        content=orjson.dumps({"delete": {"query": f"project_s:{project_identifier}"}}),
        headers={"Content-Type": "application/json"},
        timeout=None,  # noqa: S113
        verify=False,  # noqa: S501
    )

    if 200 <= res.status_code < 400:
        log.debug("Deletion was successful")
        return True
    return False


def submit_to_solr(records: list, cfg: dict) -> bool:
    solr_idx_core = cfg["solr"]["indexing_core"]
    return _submit_to_solr(records, cfg, solr_idx_core)


def _submit_to_solr(records: list, cfg: dict, core: str) -> bool:
    """
    Submits a set of records to a Solr server.

    :param records: A list of Solr records to index
    :param cfg a config object
    :return: True if successful, false if not.
    """
    solr_address = cfg["solr"]["server"]
    solr_idx_server: str = f"{solr_address}/{core}"

    log.debug("Indexing records to Solr")
    res = httpx.post(
        f"{solr_idx_server}/update",
        content=orjson.dumps(records),
        headers={"Content-Type": "application/json"},
        timeout=None,  # noqa: S113
        verify=False,  # noqa: S501
    )

    if 200 <= res.status_code < 400:
        log.debug("Indexing was successful")
        return True

    log.error("Could not index to Solr. %s: %s", res.status_code, res.text)

    return False


def commit_changes(cfg: dict) -> bool:
    solr_idx_core = cfg["solr"]["indexing_core"]
    return _commit_changes(cfg, solr_idx_core)


def _commit_changes(cfg: dict, core: str) -> bool:
    solr_address = cfg["solr"]["server"]
    solr_idx_server: str = f"{solr_address}/{core}"
    res = httpx.get(f"{solr_idx_server}/update?commit=true", timeout=None, verify=False)  # noqa: S113, S501
    if 200 <= res.status_code < 400:
        log.debug("Commit was successful")
        return True

    log.error("Could not commit to Solr. %s: %s", res.status_code, res.text)
    return False


def swap_cores(server_address: str, index_core: str, live_core: str) -> bool:
    """
    Swaps the index and live cores after indexing.

    :param server_address: The Solr server address
    :param index_core: The core that contains the newest index
    :param live_core: The core that is currently running the service
    :return: True if swap was successful; otherwise False
    """
    admconn = httpx.get(
        f"{server_address}/admin/cores?action=SWAP&core={index_core}&other={live_core}",
        timeout=None,  # noqa: S113
        verify=False,  # noqa: S501
    )

    if 200 <= admconn.status_code < 400:
        log.info("Core swap for %s and %s was successful.", index_core, live_core)
        return True

    log.error(
        "Core swap for %s and %s was not successful. Status: %s, Message: %s",
        index_core,
        live_core,
        admconn.status_code,
        admconn.text,
    )

    return False


def reload_core(server_address: str, core_name: str) -> bool:
    """
    Performs a core reload. This is a brute-force method of ensuring the core is current, since
    simply committing it doesn't seem to always work at the end of indexing.

    :param server_address: The Solr server address
    :param core_name: The name of the core to reload.
    :return: True if the reload was successful, otherwise False.
    """
    admconn = httpx.get(
        f"{server_address}/admin/cores?action=RELOAD&core={core_name}",
        timeout=None,  # noqa: S113
        verify=False,  # noqa: S501
    )

    if 200 <= admconn.status_code < 400:
        log.info("Core reload for %s was successful.", core_name)
        return True

    log.error(
        "Core reload for %s was not successful. Status: %s", core_name, admconn.text
    )
    return False


def exists(document_id: str, cfg: dict) -> bool:
    solr_address = cfg["solr"]["server"]
    solr_core = cfg["solr"]["indexing_core"]
    solr_idx_server: str = f"{solr_address}/{solr_core}"

    res = httpx.get(
        f"{solr_idx_server}/get?id={document_id}&fl=id",
        timeout=None,  # noqa: S113
        verify=False,  # noqa: S501
    )
    if 200 <= res.status_code < 400:
        json_body = res.json()
        return "doc" in json_body and json_body["doc"] is not None

    log.error("Error checking Solr. %s: %s", res.status_code, res.text)
    return False


def record_indexer(records: list, converter: Callable, cfg: dict) -> bool:
    idx_records = []

    for record in records:
        try:
            docs: list = converter(record, cfg)
        except RequiredFieldException:
            log.error("Could not index %s %s", record["type"], record["id"])
            continue

        idx_records.extend(docs)

    check: bool = True if cfg["dry"] else submit_to_solr(idx_records, cfg)

    if not check:
        log.error("There was an error indexing records.")

    return check
