import logging
from collections.abc import Callable
from urllib.parse import urlencode

import orjson
from pyreqwest.client import SyncClientBuilder

from indexer.exceptions import RequiredFieldException
from indexer.helpers.metrics import record_error, record_submission

log = logging.getLogger("muscat_indexer")

FINAL_RECORD_COUNT_QUERIES: dict[tuple[str, str], str] = {
    ("muscat", "sources"): "type:source AND -project_s:[* TO *]",
    ("muscat", "people"): "type:person AND -project_s:[* TO *]",
    ("muscat", "places"): "type:place AND -project_s:[* TO *]",
    ("muscat", "institutions"): "type:institution AND -project_s:[* TO *]",
    (
        "muscat",
        "printed_holdings",
    ): "type:holding AND source_type_s:printed AND -project_s:[* TO *]",
    (
        "muscat",
        "manuscript_holdings",
    ): "type:holding AND source_type_s:manuscript AND -project_s:[* TO *]",
    ("muscat", "subjects"): "type:subject AND -project_s:[* TO *]",
    ("muscat", "festivals"): "type:liturgical_festival AND -project_s:[* TO *]",
    ("muscat", "digital-objects"): "type:dobject AND -project_s:[* TO *]",
    ("muscat", "works"): "type:work AND -project_s:[* TO *]",
    ("muscat", "publications"): "type:publication AND -project_s:[* TO *]",
    ("muscat", "inventory-items"): "type:inventory_item AND -project_s:[* TO *]",
    ("muscat", "tombstones"): "type:tombstone AND -project_s:[* TO *]",
    (
        "muscat",
        "source_incipits",
    ): "type:incipit AND parent_type_s:source AND -project_s:[* TO *]",
    (
        "muscat",
        "work_incipits",
    ): "type:incipit AND parent_type_s:work AND -project_s:[* TO *]",
    ("diamm", "sources"): "project_s:diamm AND type:source",
    ("diamm", "institutions"): "project_s:diamm AND type:institution",
    ("diamm", "people"): "project_s:diamm AND type:person",
    ("cantus", "sources"): "project_s:cantus AND type:source",
    ("cantus", "institutions"): "project_s:cantus AND type:institution",
}


def get_final_record_counts(cfg: dict) -> tuple[dict[tuple[str, str], int], int]:
    """Return final Solr counts and the number of queries that failed."""
    server = cfg["solr"]["server"]
    core = cfg["indexing_core"]
    counts: dict[tuple[str, str], int] = {}
    errors = 0

    with SyncClientBuilder().build() as client:
        for label, query in FINAL_RECORD_COUNT_QUERIES.items():
            params = urlencode({"q": query, "rows": 0, "wt": "json"})
            response = client.get(f"{server}/{core}/select?{params}").build().send()
            if 200 <= response.status < 400:
                try:
                    counts[label] = int(response.json()["response"]["numFound"])
                    continue
                except (KeyError, TypeError, ValueError):
                    pass

            log.error("Could not get final Solr count for %s/%s", *label)
            errors += 1

    return counts, errors


def empty_solr_core(cfg: dict) -> bool:
    idx_core = cfg["solr"]["indexing_core"]
    return _empty_solr_core(cfg, idx_core)


def _empty_solr_core(cfg: dict, core: str) -> bool:
    solr_address = cfg["solr"]["server"]
    solr_idx_server: str = f"{solr_address}/{core}"

    with SyncClientBuilder().build() as client:
        res = (
            client.post(f"{solr_idx_server}/update?commit=true")
            .headers({"Content-Type": "application/json"})
            .body_bytes(orjson.dumps({"delete": {"query": "*:*"}}))
            .build()
            .send()
        )

    if 200 <= res.status < 400:
        log.debug("Deletion was successful")
        return True
    return False


def empty_project_records(project_identifier: str, cfg: dict) -> bool:
    solr_address = cfg["solr"]["server"]
    idx_core = cfg["indexing_core"]
    solr_idx_server: str = f"{solr_address}/{idx_core}"

    with SyncClientBuilder().build() as client:
        res = (
            client.post(f"{solr_idx_server}/update?commit=true")
            .headers({"Content-Type": "application/json"})
            .body_bytes(
                orjson.dumps({"delete": {"query": f"project_s:{project_identifier}"}})
            )
            .build()
            .send()
        )

    if 200 <= res.status < 400:
        log.debug("Deletion was successful")
        return True
    return False


def submit_to_solr(records: list, cfg: dict) -> bool:
    solr_idx_core = cfg["indexing_core"]
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
    with SyncClientBuilder().build() as client:
        res = (
            client.post(f"{solr_idx_server}/update")
            .headers({"Content-Type": "application/json"})
            .body_bytes(orjson.dumps(records))
            .build()
            .send()
        )

    if 200 <= res.status < 400:
        log.debug("Indexing was successful")
        record_submission(cfg, successful=True)
        return True

    log.error("Could not index to Solr. %s: %s", res.status, res.text())
    record_submission(cfg, successful=False)

    return False


def commit_changes(cfg: dict) -> bool:
    solr_idx_core = cfg["indexing_core"]
    return _commit_changes(cfg, solr_idx_core)


def _commit_changes(cfg: dict, core: str) -> bool:
    solr_address = cfg["solr"]["server"]
    solr_idx_server: str = f"{solr_address}/{core}"
    with SyncClientBuilder().build() as client:
        res = client.get(f"{solr_idx_server}/update?commit=true").build().send()
    if 200 <= res.status < 400:
        log.debug("Commit was successful")
        return True

    log.error("Could not commit to Solr. %s: %s", res.status, res.text())
    return False


def swap_cores(server_address: str, index_core: str, live_core: str) -> bool:
    """
    Swaps the index and live cores after indexing.

    :param server_address: The Solr server address
    :param index_core: The core that contains the newest index
    :param live_core: The core that is currently running the service
    :return: True if swap was successful; otherwise False
    """
    with SyncClientBuilder().build() as client:
        admconn = (
            client.get(
                f"{server_address}/admin/cores?action=SWAP&core={index_core}&other={live_core}"
            )
            .build()
            .send()
        )

    if 200 <= admconn.status < 400:
        log.info("Core swap for %s and %s was successful.", index_core, live_core)
        return True

    log.error(
        "Core swap for %s and %s was not successful. Status: %s, Message: %s",
        index_core,
        live_core,
        admconn.status,
        admconn.text(),
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
    with SyncClientBuilder().build() as client:
        admconn = (
            client.get(f"{server_address}/admin/cores?action=RELOAD&core={core_name}")
            .build()
            .send()
        )

    if 200 <= admconn.status < 400:
        log.info("Core reload for %s was successful.", core_name)
        return True

    log.error(
        "Core reload for %s was not successful. Status: %s", core_name, admconn.text()
    )
    return False


def exists(document_id: str, cfg: dict) -> bool:
    solr_address = cfg["solr"]["server"]
    solr_core = cfg["indexing_core"]
    solr_idx_server: str = f"{solr_address}/{solr_core}"

    with SyncClientBuilder().build() as client:
        res = client.get(f"{solr_idx_server}/get?id={document_id}&fl=id").build().send()
    if 200 <= res.status < 400:
        json_body = res.json()
        return "doc" in json_body and json_body["doc"] is not None

    log.error("Error checking Solr. %s: %s", res.status, res.text())
    return False


def get_existing_document_ids(document_ids: list[str], cfg: dict) -> set[str]:
    if not document_ids:
        return set()

    solr_address = cfg["solr"]["server"]
    solr_core = cfg["indexing_core"]
    solr_idx_server: str = f"{solr_address}/{solr_core}"
    existing_ids: set[str] = set()
    chunk_size = 100

    with SyncClientBuilder().build() as client:
        for i in range(0, len(document_ids), chunk_size):
            chunk = document_ids[i : i + chunk_size]
            ids = ",".join(chunk)
            res = (
                client.post(f"{solr_idx_server}/get")
                .headers({"Content-Type": "application/x-www-form-urlencoded"})
                .body_text(f"ids={ids}&fl=id")
                .build()
                .send()
            )

            if 200 <= res.status < 400:
                docs = res.json().get("response", {}).get("docs", [])
                existing_ids.update(doc["id"] for doc in docs if "id" in doc)
                continue

            log.error("Error checking Solr in batch. %s: %s", res.status, res.text())
            return set()

    return existing_ids


def record_indexer(records: list, converter: Callable, cfg: dict) -> bool:
    idx_records = []

    for record in records:
        try:
            docs: list = converter(record, cfg)
        except RequiredFieldException:
            log.error("Could not index %s %s", record["type"], record["id"])
            record_error(cfg)
            continue

        idx_records.extend(docs)

    check: bool = True if cfg["dry"] else submit_to_solr(idx_records, cfg)

    if not check:
        log.error("There was an error indexing records.")

    return check
