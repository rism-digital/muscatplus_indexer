import logging
from typing import Optional

import orjson

from diamm_indexer.helpers.identifiers import transform_rism_id, RELATOR_MAP, COUNTRY_SIGLUM_MAP
from indexer.helpers.identifiers import ProjectIdentifiers, COUNTRY_CODE_MAPPING
from indexer.helpers.solr import exists

log = logging.getLogger("muscat_indexer")


def update_rism_institution_document(record, cfg: dict) -> Optional[dict]:
    document_id: Optional[str] = transform_rism_id(record.get("rism_id"))
    if not document_id:
        return None

    if not exists(document_id, cfg):
        log.error("Institution %s does not exist in RISM (DIAMM ID: Organization %s", document_id, record["id"])
        return None

    diamm_id = record['id']
    entry: dict = {
        "id": f"{diamm_id}",
        "type": "institution",
        "project_type": f'{record.get("project_type")}',
        "project": "diamm",
        "label": f"{record.get('name')}"
    }

    entry_s: str = orjson.dumps(entry).decode("utf-8")

    return {
        "id": document_id,
        "has_external_record_b": {"set": True},
        "external_records_jsonm": {"add-distinct": entry_s}
    }


def create_organization_index_document(record, cfg: dict) -> dict:
    log.debug("Indexing organization %s", record['name'])
    institution_id: str = f"diamm_organization_{record['id']}"
    raw_locations: Optional[str] = record.get("location")
    location_map: dict = {}
    if raw_locations:
        locs = raw_locations.split('\n')[0]
        city, country, country_id = locs.split("||")
        siglum_pfx: str = COUNTRY_SIGLUM_MAP.get(country_id, "")
        country_names: list = COUNTRY_CODE_MAPPING.get(siglum_pfx, [])

        location_map["city_s"] = city
        location_map["country_codes_sm"] = [siglum_pfx] if siglum_pfx else None
        location_map["country_names_sm"] = country_names if country_names else None

    related_sources: list = _get_related_sources_json(record['related_sources'])

    d = {
        "id": institution_id,
        "institution_id": institution_id,
        "type": "institution",
        "project_type_s": "organization",
        "project_s": ProjectIdentifiers.DIAMM,
        "record_uri_sni": f"https://www.diamm.ac.uk/organizations/{record['id']}",
        "name_s": record['name'],
        "has_siglum_b": False,
        "related_sources_json": orjson.dumps(related_sources).decode('utf-8') if related_sources else None,
        "created": record["created"].strftime("%Y-%m-%dT%H:%M:%SZ"),
        "updated": record["updated"].strftime("%Y-%m-%dT%H:%M:%SZ"),
    }

    d.update(location_map)

    return d


def _get_related_sources_ids(sources: Optional[str]) -> Optional[list]:
    if not sources:
        return None

    sources_raw: list[str] = sources.split("\n")
    return [f"diamm_source_{o.split('||')[-1]}" for o in sources_raw]


def _get_related_sources_json(sources: Optional[str]) -> list[dict]:
    if not sources:
        return []

    sources_raw: list[str] = sources.split("\n")

    sources_json: list = []
    for source in sources_raw:
        siglum, shelfmark, name, relnum, certain, source_id = source.split("||")
        title = name if name else "[No title]"
        relator_code = RELATOR_MAP.get(relnum, "unk")

        d = {
            "id": f"diamm_source_{source_id}",
            "type": "source",
            "project": ProjectIdentifiers.DIAMM,
            "project_type": "sources",
            "source_id": f"diamm_source_{source_id}",
            "title": [{"title": title,
                       "source_type": "Manuscript copy",
                       "holding_shelfmark": shelfmark,
                       "holding_siglum": siglum}],
            "relationship": relator_code
        }

        sources_json.append(d)

    return sources_json
