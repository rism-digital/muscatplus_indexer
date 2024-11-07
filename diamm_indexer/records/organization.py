import logging
from typing import Optional

import orjson

from diamm_indexer.helpers.identifiers import COUNTRY_SIGLUM_MAP
from diamm_indexer.helpers.utilities import get_related_sources_json
from indexer.helpers.identifiers import COUNTRY_CODE_MAPPING, ProjectIdentifiers

log = logging.getLogger("muscat_indexer")


def create_organization_index_document(record, cfg: dict) -> list[dict]:
    log.debug("Indexing organization %s", record["name"])
    institution_id: str = f"diamm_organization_{record['id']}"
    raw_locations: Optional[str] = record.get("location")
    location_map: dict = {}
    if raw_locations:
        locs = raw_locations.split("\n")[0]
        city, _country, country_id = locs.split("||")
        siglum_pfx: str = COUNTRY_SIGLUM_MAP.get(country_id, "")
        country_names: list = COUNTRY_CODE_MAPPING.get(siglum_pfx, [])

        location_map["city_s"] = city
        location_map["country_codes_sm"] = [siglum_pfx] if siglum_pfx else None
        location_map["country_names_sm"] = country_names if country_names else None

    related_sources: list = get_related_sources_json(record["related_sources"])
    copied_sources: list = get_related_sources_json(record["copied_sources"])
    all_related_sources = related_sources + copied_sources
    num_related_sources = len(all_related_sources)

    d = {
        "id": institution_id,
        "institution_id": institution_id,
        "type": "institution",
        "project_type_s": "organization",
        "project_s": ProjectIdentifiers.DIAMM,
        "record_uri_sni": f"https://www.diamm.ac.uk/organizations/{record['id']}",
        "name_s": record["name"],
        "has_siglum_b": False,
        "total_sources_i": num_related_sources,
        "related_sources_json": orjson.dumps(all_related_sources).decode("utf-8")
        if all_related_sources
        else None,
        "created": record["created"].strftime("%Y-%m-%dT%H:%M:%SZ"),
        "updated": record["updated"].strftime("%Y-%m-%dT%H:%M:%SZ"),
    }

    d.update(location_map)

    return [d]


def _get_related_sources_ids(sources: Optional[str]) -> Optional[list]:
    if not sources:
        return None

    sources_raw: list[str] = sources.split("\n")
    return [f"diamm_source_{o.split('||')[-1]}" for o in sources_raw]
