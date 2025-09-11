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
    rism_id: str = marc_record["001"].value()
    institution_id: str = f"institution_{rism_id}"

    related_source_ids: list[int] = (
        orjson.loads(r) if (r := record["source_count"]) else []
    )
    source_count = len(related_source_ids)
    # source_count: int = record.get("source_count", 0)
    related_holding_source_ids: list[int] = (
        orjson.loads(h) if (h := record["holdings_count"]) else []
    )
    holdings_count: int = len(related_holding_source_ids)
    # holdings_count: int = record.get("holdings_count", 0)

    related_source_other_ids: list[int] = (
        orjson.loads(o) if (o := record["other_count"]) else []
    )
    other_count: int = len(related_source_other_ids)
    # other_count: int = record.get("other_count", 0)

    related_source_other_holding_ids: list[int] = (
        orjson.loads(p) if (p := record["other_holdings_count"]) else []
    )
    # other_holdings_count: int = len(related_source_other_holding_ids)
    # other_holdings_count: int = record.get("other_holdings_count", 0)

    all_related_ids = set(
        related_source_ids
        + related_holding_source_ids
        + related_source_other_ids
        + related_source_other_holding_ids
    )
    total_count = len(all_related_ids)

    # total_count: int = (
    #     source_count + holdings_count + other_count + other_holdings_count
    # )
    people_contribution_count: int = record.get("people_contribution_count", 0)

    related_institutions: list = (
        orjson.loads(ii) if (ii := record["institution_relationships"]) else []
    )

    now_in: list[dict] = []
    now_in_sigla: list = []
    contains: list[dict] = []
    contains_sigla: list = []
    related: list = []
    related_sigla: list = []

    for i, reli in enumerate(related_institutions, 1):
        a_institution = {
            "institution_id": f"{reli['a_id']}",
            "type": "institution",
            "name": f"{reli['a_name']}",
            "place": f"{reli['a_place']}",
            "siglum": f"{reli['a_siglum']}",
            "this_id": institution_id,
            "this_type": "institution",
        }

        b_institution = {
            "institution_id": f"{reli['b_id']}",
            "type": "institution",
            "name": f"{reli['b_name']}",
            "place": f"{reli['b_place']}",
            "siglum": reli["b_siglum"],
            "this_id": institution_id,
            "this_type": "institution",
        }

        if bp := reli.get("b_place"):
            b_institution["place"] = bp

        marc_tag: str = reli["marc_tag"]
        a_now_in_b: bool = reli["a_now_in_b"]
        b_contains_a: bool = reli["b_contains_a"]
        a_siglum = reli["a_siglum"]
        b_siglum = reli["b_siglum"]

        if marc_tag == "580" and a_now_in_b:
            b_institution["id"] = f"{i}"
            b_institution["relationship"] = "now-in"
            now_in.append({k: v for k, v in b_institution.items() if v})
            if b_siglum:
                now_in_sigla.append(b_siglum)
        elif marc_tag == "580" and b_contains_a:
            a_institution["id"] = f"{i}"
            a_institution["relationship"] = "contained-by"
            contains.append({k: v for k, v in a_institution.items() if v})
            if a_siglum:
                contains_sigla.append(a_siglum)
        # The query will pick up on bidirectional 710s (institutions that mention each other)
        # so guarding it with the `a_now_in_b` flag will ensure only one of those two relationships
        # are selected.
        elif marc_tag == "710" and a_now_in_b:
            b_institution["id"] = f"{i}"
            b_institution["relationship"] = "xi"
            related.append({k: v for k, v in b_institution.items() if v})
            if b_siglum:
                related_sigla.append(b_siglum)
        else:
            continue

    has_digital_objects: bool = record.get("digital_objects") is not None
    digital_object_ids: list[str] = (
        orjson.loads(d) if (d := record.get("digital_objects")) else []
    )
    roles: list[str] = (
        orjson.loads(s) if (s := record.get("source_relationships")) else []
    )

    publication_entries: list = (
        orjson.loads(d) if (d := record.get("publication_entries")) else []
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
        "legacy_id": f"ks{rism_id}",
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
        "related_institutions_json": orjson.dumps(related).decode("utf-8")
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
