import logging
from typing import TypedDict, Optional

import pymarc
import orjson
import yaml

from indexer.helpers.marc import create_marc
from indexer.helpers.profiles import process_marc_profile
from indexer.helpers.utilities import normalize_id
from indexer.processors import institution as institution_processor

log = logging.getLogger("muscat_indexer")
institution_profile: dict = yaml.full_load(open("profiles/institutions.yml", "r"))


class InstitutionIndexDocument(TypedDict):
    id: str
    type: str
    institution_id: str
    name_s: str
    city_s: Optional[str]
    siglum_s: Optional[str]
    country_code_s: Optional[str]
    alternate_names_sm: Optional[list[str]]
    institution_types_sm: Optional[list[str]]
    website_s: Optional[str]
    external_ids: Optional[list[str]]
    related_people_json: Optional[str]
    related_places_json: Optional[str]
    related_institutions_json: Optional[str]
    location_loc: Optional[str]


def create_institution_index_document(
    record: dict, cfg: dict
) -> InstitutionIndexDocument:
    marc_record: pymarc.Record = create_marc(record["marc_source"])
    rism_id: str = normalize_id(marc_record["001"].value())
    institution_id: str = f"institution_{rism_id}"

    source_count: int = record.get("source_count", 0)
    holdings_count: int = record.get("holdings_count", 0)
    other_count: int = record.get("other_count", 0)
    total_count: int = record.get("total_source_count", 0)

    related_institutions = record.get("related_institutions")
    inst_lookup: dict = {}
    if related_institutions:
        all_related_institutions: list = related_institutions.split("\n")
        for inst in all_related_institutions:
            components: list = inst.split("|")
            if len(components) == 2:
                inst_lookup[components[0]] = {"name": components[1]}
            elif len(components) == 3:
                inst_lookup[components[0]] = {
                    "name": components[2],
                    "siglum": components[1],
                }
            else:
                continue

    now_in: Optional[list[dict]] = _get_now_in_json(
        marc_record, inst_lookup, institution_id
    )
    has_digital_objects: bool = record.get("digital_objects") is not None
    digital_object_ids: list[str] = (
        [f"dobject_{i}" for i in record["digital_objects"].split(",") if i]
        if record.get("digital_objects")
        else []
    )

    institution_core: dict = {
        "id": institution_id,
        "type": "institution",
        "institution_id": institution_id,
        "rism_id": rism_id,
        "has_digital_objects_b": has_digital_objects,
        "digital_object_ids": digital_object_ids,
        "has_siglum_b": True if record.get("siglum") else False,
        "source_count_i": source_count if rism_id != "40009305" else 0,
        "holdings_count_i": holdings_count if rism_id != "40009305" else 0,
        "other_count_i": other_count if rism_id != "40009305" else 0,
        "total_sources_i": total_count if rism_id != "40009305" else 0,
        "now_in_json": orjson.dumps(now_in).decode("utf-8") if now_in else None,
        "created": record["created"].strftime("%Y-%m-%dT%H:%M:%SZ"),
        "updated": record["updated"].strftime("%Y-%m-%dT%H:%M:%SZ"),
    }

    additional_fields: dict = process_marc_profile(
        institution_profile, institution_id, marc_record, institution_processor
    )
    institution_core.update(additional_fields)

    return institution_core


def _get_now_in_json(
    record: pymarc.Record, related_institutions: dict, this_id: str
) -> Optional[list[dict]]:
    if "580" not in record:
        return None

    now_in_fields: list[pymarc.Field] = record.get_fields("580")
    all_entries: list = []

    for num, entry in enumerate(now_in_fields, 1):
        institution_id = entry.get("0")
        if not institution_id:
            log.warning(f"Got a 'now in' record with no identifier.")
            continue

        if institution_id not in related_institutions:
            log.warning("Could not find a 'now in' institution for %s", institution_id)
            continue

        institution_info: dict = related_institutions.get(institution_id)
        now_in: dict = {
            "id": f"{num}",
            "type": "institution",
            "institution_id": f"institution_{institution_id}",
            "name": f"{institution_info.get('name')}",
            "relationship": "now-in",
            "this_id": this_id,
            "this_type": "institution",
        }

        if "siglum" in institution_info:
            now_in["siglum"] = institution_info.get("siglum")

        all_entries.append(now_in)

    return all_entries
