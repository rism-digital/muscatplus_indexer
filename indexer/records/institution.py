import logging
from typing import TypedDict

import orjson
import pymarc
import yaml

from indexer.helpers.marc import create_marc
from indexer.helpers.profiles import process_marc_profile
from indexer.helpers.utilities import (
    get_bibliographic_reference_titles,
    get_bibliographic_references_json,
    get_related_json,
    normalize_id,
    process_related_institutions,
)
from indexer.processors import institution as institution_processor

log = logging.getLogger("muscat_indexer")

with open("profiles/institutions.yml") as pi:
    institution_profile: dict = yaml.full_load(pi)


class InstitutionIndexDocument(TypedDict):
    id: str
    type: str
    institution_id: str
    name_s: str
    city_s: str | None
    siglum_s: str | None
    country_code_s: str | None
    alternate_names_sm: list[str] | None
    institution_types_sm: list[str] | None
    website_s: str | None
    external_ids: list[str] | None
    related_people_json: str | None
    related_places_json: str | None
    related_institutions_json: str | None
    location_loc: str | None


def create_institution_index_document(record: dict, cfg: dict) -> dict[str, object]:
    marc_record: pymarc.Record = create_marc(record["marc_source"])
    rism_id: str = normalize_id(marc_record["001"].value())
    institution_id: str = f"institution_{rism_id}"

    source_count: int = record.get("source_count", 0)
    holdings_count: int = record.get("holdings_count", 0)
    other_count: int = record.get("other_count", 0)
    total_count: int = record.get("total_source_count", 0)
    people_contribution_count: int = record.get("people_contribution_count", 0)

    now_in: list[dict] | None = None
    now_in_sigla: list | None = None
    now_in_institutions: str | None = record.get("now_in_institutions")
    if now_in_institutions:
        all_now_in_institutions: list = orjson.loads(now_in_institutions)
        now_in_institution_lookup: dict = process_related_institutions(
            all_now_in_institutions
        )

        now_in = get_related_json(
            marc_record, now_in_institution_lookup, institution_id, "institution", "580"
        )
        now_in_sigla = [
            s["siglum"]
            for k, s in now_in_institution_lookup.items()
            if s and "siglum" in s
        ]

    contains: list[dict] | None = None
    contains_sigla: list | None = None
    contains_institutions: str | None = record.get("contains_institutions")
    if contains_institutions:
        all_contains_institutions: list = orjson.loads(contains_institutions)
        contains_institution_lookup: dict = process_related_institutions(
            all_contains_institutions
        )
        contains = _get_contains_json(contains_institution_lookup, institution_id)
        contains_sigla = [
            s["siglum"]
            for k, s in contains_institution_lookup.items()
            if s and "siglum" in s
        ]

    related = None
    related_sigla = None
    related_institutions: str | None = record.get("related_institutions")
    if related_institutions:
        all_related_institutions: list = orjson.loads(related_institutions)
        related_institutions_lookup: dict = process_related_institutions(
            all_related_institutions
        )
        related = get_related_json(
            marc_record, related_institutions_lookup, institution_id, "institution", "710"
        )
        related_sigla = [
            s["siglum"]
            for k, s in related_institutions_lookup.items()
            if s and "siglum" in s
        ]

    has_digital_objects: bool = record.get("digital_objects") is not None
    digital_object_ids: list[str] = (
        orjson.loads(d)
        if (d := record.get("digital_objects"))
        else []
    )
    roles: list[str] = (
        orjson.loads(s)
        if (s := record.get("source_relationships"))
        else []
    )

    publication_entries: list = (
        orjson.loads(d)
        if (d := record.get("publication_entries"))
        else []
    )
    bibliographic_references: list[dict] | None = get_bibliographic_references_json(
        marc_record, "670", publication_entries
    )
    bibliographic_references_json = (
        orjson.dumps(bibliographic_references).decode("utf-8")
        if bibliographic_references
        else None
    )
    bibliographic_reference_titles: list[str] | None = (
        get_bibliographic_reference_titles(publication_entries)
    )

    institution_core: dict = {
        "id": institution_id,
        "type": "institution",
        "institution_id": institution_id,
        "rism_id": rism_id,
        "full_rism_id": f"institutions/{rism_id}",
        "roles_sm": roles,
        "has_digital_objects_b": has_digital_objects,
        "digital_object_ids": digital_object_ids,
        "has_siglum_b": bool(record.get("siglum")),
        "contains_sigla_sm": contains_sigla,
        "now_in_sigla_sm": now_in_sigla,
        "related_institution_sigla_sm": related_sigla,
        "source_count_i": source_count if rism_id != "40009305" else 0,
        "holdings_count_i": holdings_count if rism_id != "40009305" else 0,
        "other_count_i": other_count if rism_id != "40009305" else 0,
        "total_sources_i": total_count if rism_id != "40009305" else 0,
        "people_contribution_count_i": people_contribution_count,
        "num_sources_s": _get_num_sources_facet(total_count),
        "bibliographic_references_json": bibliographic_references_json,
        "bibliographic_references_sm": bibliographic_reference_titles,
        "now_in_json": orjson.dumps(now_in).decode("utf-8") if now_in else None,
        "contains_json": orjson.dumps(contains).decode("utf-8") if contains else None,
        "related_institutions_json": orjson.dumps(related).decode("utf-8") if related else None
        if related
        else None,
        "created": record["created"].strftime("%Y-%m-%dT%H:%M:%SZ"),
        "updated": record["updated"].strftime("%Y-%m-%dT%H:%M:%SZ"),
    }

    additional_fields: dict = process_marc_profile(
        institution_profile, institution_id, marc_record, institution_processor
    )
    institution_core.update(additional_fields)

    return institution_core


def _get_contains_json(contained_institutions: dict, this_id: str) -> list[dict] | None:
    all_entries: list = []

    for inst_id, inst_info in contained_institutions.items():
        contained_by: dict = {
            "id": f"{inst_id}",
            "type": "institution",
            "institution_id": f"institution_{inst_id}",
            "name": inst_info["name"],
            "relationship": "contained-by",
            "this_id": this_id,
            "this_type": "institution",
        }

        if "siglum" in inst_info:
            contained_by["siglum"] = inst_info["siglum"]

        if "place" in inst_info:
            contained_by["place"] = inst_info["place"]

        all_entries.append(contained_by)

    return all_entries


def _get_num_sources_facet(num: int) -> str | None:
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
