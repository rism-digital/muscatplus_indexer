import logging

from indexer.helpers.identifiers import ProjectIdentifiers

log = logging.getLogger("muscat_indexer")


def create_organization_index_document(record, cfg: dict) -> dict:
    log.debug("Indexing organization %s", record['name'])
    d = {
        "id": f"diamm_organization_{record['id']}",
        "type": "institution",
        "project_s": ProjectIdentifiers.DIAMM,
        "record_uri_sni": f"https://www.diamm.ac.uk/organizations/{record['id']}",
        "name_s": record['name'],
        "city_s": record['city_name'],
        "has_siglum_b": False,
        "created": record["created"].strftime("%Y-%m-%dT%H:%M:%SZ"),
        "updated": record["updated"].strftime("%Y-%m-%dT%H:%M:%SZ"),
    }

    return d
