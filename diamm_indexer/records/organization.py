import logging
from typing import Optional

import orjson

from diamm_indexer.helpers.identifiers import transform_rism_id
from indexer.helpers.identifiers import ProjectIdentifiers
from indexer.helpers.solr import exists

log = logging.getLogger("muscat_indexer")


def update_rism_institution_document(record, cfg: dict) -> Optional[dict]:
    document_id: Optional[str] = transform_rism_id(record.get("rism_id"))
    if not document_id:
        return None

    if not exists(document_id, cfg):
        log.error("Institution %s does not exist in RISM", document_id)
        return None
    #
    # diamm_id = record['id']
    # entry: dict = {
    #     "id": f"{diamm_id}",
    #     "type": "institution",
    #     "project": "diamm",
    #     "label": record["name"]
    # }
    #
    # entry_s: str = orjson.dumps(entry).decode("utf-8")
    return {
        "id": document_id,
        "has_external_record_b": {"set": True},
        # "external_records_jsonm": {"add-distinct": entry_s}
    }


def create_organization_index_document(record, cfg: dict) -> dict:
    log.debug("Indexing organization %s", record['name'])
    d = {
        "id": f"diamm_organization_{record['id']}",
        "type": "institution",
        "project_type_s": "organization",
        "project_s": ProjectIdentifiers.DIAMM,
        "record_uri_sni": f"https://www.diamm.ac.uk/organizations/{record['id']}",
        "name_s": record['name'],
        "city_s": record['city_name'],
        "has_siglum_b": False,
        "created": record["created"].strftime("%Y-%m-%dT%H:%M:%SZ"),
        "updated": record["updated"].strftime("%Y-%m-%dT%H:%M:%SZ"),
    }

    return d
