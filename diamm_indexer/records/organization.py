import logging

log = logging.getLogger("muscat_indexer")


def create_organization_index_document(record, cfg: dict) -> dict:
    log.debug("Indexing organization %s", record['name'])
    d = {
        "id": f"diamm_organization_{record['id']}",
        "type": "institution",
        "db_s": "diamm",
        "name_s": record['name'],
        "city_s": record['city_name'],
        "has_siglum_b": False,
        "created": record["created"].strftime("%Y-%m-%dT%H:%M:%SZ"),
        "updated": record["updated"].strftime("%Y-%m-%dT%H:%M:%SZ"),
    }

    return d
