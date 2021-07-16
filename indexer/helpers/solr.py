import logging
from typing import List, Dict

import httpx
import ujson
import yaml

log = logging.getLogger("muscat_indexer")

idx_config: Dict = yaml.full_load(open('index_config.yml', 'r'))
solr_address = idx_config['solr']['server']
solr_idx_core = idx_config['solr']['indexing_core']
solr_idx_server: str = f"{solr_address}/{solr_idx_core}"


def empty_solr_core() -> bool:
    res = httpx.post(f"{solr_idx_server}/update",
                     data=ujson.dumps({"delete": {"query": "*:*"}}),
                     headers={"Content-Type": "application/json"})

    if 200 <= res.status_code < 400:
        log.debug("Deletion was successful")
        return True
    return False


def submit_to_solr(records: List) -> bool:
    """
    Submits a set of records to a Solr server.

    :param records: A list of Solr records to index
    :return: True if successful, false if not.
    """
    log.debug("Indexing records to Solr")
    res = httpx.post(f"{solr_idx_server}/update",
                     data=ujson.dumps(records),
                     headers={"Content-Type": "application/json"},
                     timeout=None)

    if 200 <= res.status_code < 400:
        log.debug("Indexing was successful")
        return True

    log.error("Could not index to Solr. %s: %s", res.status_code, res.text)

    return False


def commit_changes():
    log.info("Committing changes")
    res = httpx.get(f"{solr_idx_server}/update?commit=true",
                    timeout=None)
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
    admconn = httpx.get(f"{server_address}/admin/cores?action=SWAP&core={index_core}&other={live_core}",
                        timeout=None)

    if 200 <= admconn.status_code < 400:
        log.info("Core swap for %s and %s was successful.", index_core, live_core)
        return True

    log.error("Core swap for %s and %s was not successful. Status: %s, Message: %s",
              index_core, live_core, admconn.status_code, admconn.text)

    return False


def reload_core(server_address: str, core_name: str) -> bool:
    """
    Performs a core reload. This is a brute-force method of ensuring the core is current, since
    simply committing it doesn't seem to always work at the end of indexing.

    :param server_address: The Solr server address
    :param core_name: The name of the core to reload.
    :return: True if the reload was successful, otherwise False.
    """
    admconn = httpx.get(f"{server_address}/admin/cores?action=RELOAD&core={core_name}",
                        timeout=None)

    if 200 <= admconn.status_code < 400:
        log.info("Core reload for %s was successful.", core_name)
        return True

    log.error("Core reload for %s was not successful. Status: %s", core_name, admconn.text)
    return False
