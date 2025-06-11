import argparse
import logging
import sys

import httpx
import orjson
import yaml

log = logging.getLogger("mp_indexer")


def _empty_solr_core(cfg: dict, solr_core: str, delete_query: str = "*:*") -> bool:
    solr_address = cfg["solr"]["server"]
    solr_idx_server: str = f"{solr_address}/{solr_core}"

    res = httpx.post(
        f"{solr_idx_server}/update?commit=true",
        content=orjson.dumps({"delete": {"query": delete_query}}),
        headers={"Content-Type": "application/json"},
        timeout=None,  # noqa: S113
        verify=False,  # noqa: S501
    )

    if 200 <= res.status_code < 400:
        log.debug("Deletion was successful")
        return True
    return False

if __name__ == "__main__":
    description: str = "Empties all records in a given solr core."
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "core",
        help="A solr core to empty. Should correspond to values in the silo_s field in Solr.",
    )
    parser.add_argument(
        "query",
        default="*:*"
    )
    args = parser.parse_args()

    configuration: dict = yaml.full_load(open("./index_config.yml"))  # nosec
    solr_server = configuration["solr"]["server"]
    core = args.core

    confirm = input(
        f"WARNING: This will completely empty the Solr core {core}. Type 'yes' to continue; Anything else will exit. >> "
    )  # nosec
    if confirm != "yes":
        sys.exit(0)
    else:
        res = _empty_solr_core(configuration, core, args.query)
        if not res:
            print("Uh oh! Something went wrong.")
            sys.exit(1)

        print("All records have been deleted with \U00002764.")
