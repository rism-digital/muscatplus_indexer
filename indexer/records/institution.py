import logging
from typing import TypedDict, Optional

import pymarc
import orjson
import yaml

from indexer.helpers.marc import create_marc
from indexer.helpers.profiles import process_marc_profile
from indexer.helpers.utilities import (
    normalize_id,
    get_bibliographic_references_json,
    get_bibliographic_reference_titles
)
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

    now_in: Optional[list[dict]] = None
    now_in_sigla: Optional[list] = None
    related_institutions = record.get("related_institutions")
    if related_institutions:
        all_related_institutions: list = related_institutions.split("\n")
        related_institution_lookup: dict = _process_related_institutions(all_related_institutions)

        now_in = _get_now_in_json(
            marc_record, related_institution_lookup, institution_id
        )
        now_in_sigla = [s['siglum'] for k, s in related_institution_lookup.items() if s and 'siglum' in s]

    contains: Optional[list[dict]] = None
    contains_sigla: Optional[list] = None
    referring_institutions: str = record.get("referring_institutions")
    if referring_institutions:
        all_referring_institutions: list = referring_institutions.split("\n")
        referring_institution_lookup: dict = _process_related_institutions(all_referring_institutions)
        contains = _get_contains_json(
            referring_institution_lookup, institution_id
        )
        contains_sigla = [s['siglum'] for k, s in referring_institution_lookup.items() if s and 'siglum' in s]

    has_digital_objects: bool = record.get("digital_objects") is not None
    digital_object_ids: list[str] = (
        [f"dobject_{i}" for i in record["digital_objects"].split(",") if i]
        if record.get("digital_objects")
        else []
    )
    roles: list[str] = (
        [s.strip() for s in record['source_relationships'].split(",") if s]
        if record.get("source_relationships") else []
    )

    publication_entries: list = (
        list({n.strip() for n in d.split("\n") if n and n.strip()}) if (d := record.get("publication_entries")) else []
    )
    bibliographic_references: Optional[list[dict]] = get_bibliographic_references_json(
        marc_record, "670", publication_entries
    )
    bibliographic_references_json = (
        orjson.dumps(bibliographic_references).decode("utf-8") if bibliographic_references else None
    )
    bibliographic_reference_titles: Optional[list[str]] = get_bibliographic_reference_titles(publication_entries)

    institution_core: dict = {
        "id": institution_id,
        "type": "institution",
        "institution_id": institution_id,
        "rism_id": rism_id,
        "roles_sm": roles,
        "has_digital_objects_b": has_digital_objects,
        "digital_object_ids": digital_object_ids,
        "has_siglum_b": True if record.get("siglum") else False,
        "contains_sigla_sm": contains_sigla,
        "now_in_sigla_sm": now_in_sigla,
        "source_count_i": source_count if rism_id != "40009305" else 0,
        "holdings_count_i": holdings_count if rism_id != "40009305" else 0,
        "other_count_i": other_count if rism_id != "40009305" else 0,
        "total_sources_i": total_count if rism_id != "40009305" else 0,
        "num_sources_s": _get_num_sources_facet(total_count),
        "bibliographic_references_json": bibliographic_references_json,
        "bibliographic_references_sm": bibliographic_reference_titles,
        "now_in_json": orjson.dumps(now_in).decode("utf-8") if now_in else None,
        "contains_json": orjson.dumps(contains).decode("utf-8") if contains else None,
        "created": record["created"].strftime("%Y-%m-%dT%H:%M:%SZ"),
        "updated": record["updated"].strftime("%Y-%m-%dT%H:%M:%SZ"),
    }

    additional_fields: dict = process_marc_profile(
        institution_profile, institution_id, marc_record, institution_processor
    )
    institution_core.update(additional_fields)

    return institution_core


def _process_related_institutions(institutions: list) -> dict:
    inst_lookup: dict = {}

    for inst in institutions:
        inst_id, siglum, name, place = inst.split("|")
        d = {
            "name": name
        }

        if siglum:
            d['siglum'] = siglum

        if place:
            d['place'] = place

        inst_lookup[inst_id] = d

    return inst_lookup


def _get_now_in_json(
        record: pymarc.Record,
        related_institutions: dict,
        this_id: str
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
            now_in["siglum"] = institution_info["siglum"]

        if "place" in institution_info:
            now_in["place"] = institution_info["place"]

        all_entries.append(now_in)

    return all_entries


def _get_contains_json(
        contained_institutions: dict,
        this_id: str
) -> Optional[list[dict]]:
    all_entries: list = []
    for inst_id, inst_info in contained_institutions.items():
        contained_by: dict = {
            "id": f"{inst_id}",
            "type": "institution",
            "institution_id": f"institution_{inst_id}",
            "name": inst_info['name'],
            "relationship": "contained-by",
            "this_id": this_id,
            "this_type": "institution"
        }

        if "siglum" in inst_info:
            contained_by["siglum"] = inst_info["siglum"]

        if "place" in inst_info:
            contained_by["place"] = inst_info["place"]

        all_entries.append(contained_by)

    return all_entries


def _get_num_sources_facet(num: int) -> Optional[str]:
    if num == 0:
        return None
    elif num == 1:
        return "1"
    elif 2 <= num <= 10:
        return "2 to 10"
    elif 11 <= num <= 100:
        return "11 to 100"
    elif num > 100:
        return "more than 100"
    else:
        return None
