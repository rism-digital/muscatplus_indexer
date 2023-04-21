import logging
from typing import Optional

log = logging.getLogger("muscat_indexer")


def create_archive_index_document(record, cfg: dict) -> dict:
    log.debug("Indexing archive %s", record['name'])
    siglum = record["siglum"]
    siglum_lookup: dict = cfg['sigla_lookup']

    rism_id: Optional[str] = None
    if siglum in siglum_lookup:
        rism_id = f"institution_{siglum_lookup[siglum]}"

    d = {
        "id": f"diamm_archive_{record['id']}",
        "type": "institution",
        "db_s": "diamm",
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
