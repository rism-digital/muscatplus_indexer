import logging
from typing import Optional

from indexer.helpers.identifiers import ProjectIdentifiers

log = logging.getLogger("muscat_indexer")


def create_archive_index_document(record, cfg: dict) -> dict:
    log.debug("Indexing archive %s", record['name'])
    rism_id_val: Optional[str] = record.get("rism_identifier")
    rism_id = rism_id_val.replace("institutions/", "institution_") if rism_id_val else None

    d = {
        "id": f"diamm_archive_{record['id']}",
        "type": "institution",
        "project_type_s": "archive",
        "project_s": ProjectIdentifiers.DIAMM,
        "rism_id": rism_id,
        "name_s": record['name'],
        "siglum_s": record['siglum'],
        "has_siglum_b": record['siglum'] is not None,
        "city_s": record["city_name"],
        "total_sources_i": record["source_count"] if record['source_count'] > 0 else None,
        "created": record["created"].strftime("%Y-%m-%dT%H:%M:%SZ"),
        "updated": record["updated"].strftime("%Y-%m-%dT%H:%M:%SZ"),
    }

    return d
