import logging
from typing import TypedDict

import orjson
import pymarc
import yaml

from indexer.helpers.marc import create_marc
from indexer.helpers.profiles import process_marc_profile
from indexer.helpers.utilities import (
    get_work_node,
)
from indexer.processors import person as person_processor

log = logging.getLogger("muscat_indexer")
person_profile: dict = yaml.full_load(open("profiles/people.yml"))  # noqa: SIM115


class PersonIndexDocument(TypedDict):
    id: str
    type: str
    person_id: str
    name_s: str | None
    date_statement_s: str | None
    other_dates_s: str | None
    variant_names_sm: list | None
    related_places_sm: list | None
    related_people_sm: list | None
    related_institutions_sm: list | None
    general_notes_sm: list | None
    additional_biography_sm: list | None
    gender_s: str | None
    roles_sm: list | None
    external_ids: list | None
    boost: int
    related_people_json: str | None
    related_places_json: str | None
    related_institutions_json: str | None
    name_variants_json: str | None
    external_resources_json: str | None


def create_person_index_document(record: dict, cfg: dict) -> dict:
    marc_record: pymarc.Record = create_marc(record["marc_source"])
    rism_id: str = marc_record["001"].value()
    person_id: str = f"person_{rism_id}"
    roles: list[str] = (
        orjson.loads(s) if (s := record.get("source_relationships")) else []
    )

    source_count: int = record.get("source_count", 0)
    holdings_count: int = record.get("holdings_count", 0)
    total_count: int = source_count + holdings_count
    has_digital_objects: bool = record.get("digital_objects") is not None
    digital_object_ids: list[str] = (
        orjson.loads(d) if (d := record.get("digital_objects")) else []
    )

    work_catalogues: list = orjson.loads(w) if (w := record["work_catalogues"]) else []
    formatted_catalogues = _get_work_catalogues(work_catalogues)
    works_catalogue_json: str | None = None
    if formatted_catalogues:
        works_catalogue_json = orjson.dumps(formatted_catalogues).decode("utf-8")

    work_nodes_json = None
    work_node_ids = None
    if work_nodes := record.get("work_nodes"):
        all_work_nodes: list[dict] = _get_work_nodes(work_nodes, person_id)
        work_node_ids = [
            wn.get("external_id") for wn in all_work_nodes if wn and "external_id" in wn
        ]
        work_nodes_json = (
            orjson.dumps(all_work_nodes).decode("utf-8") if all_work_nodes else None
        )

    related: list = []
    related_institutions: list = (
        orjson.loads(ii) if (ii := record.get("related_institutions")) else []
    )
    for i, reli in enumerate(related_institutions, 1):
        institution_record: dict = {
            "id": f"{i}",
            "institution_id": f"{reli['institution_id']}",
            "type": "institution",
            "name": f"{reli['name']}",
            "place": f"{reli['place']}",
            "siglum": reli["siglum"],
            "relationship": "xi",
            "this_id": person_id,
            "this_type": "person",
        }
        related.append({k: v for k, v in institution_record.items() if v})

    # For the source count we take the literal count *except* for the Anonymous user,
    # since that throws everything off.
    core_person: dict = {
        "type": "person",
        "id": person_id,
        "person_id": person_id,
        "rism_id": rism_id,
        "full_rism_id": f"people/{rism_id}",
        "legacy_id": f"pe{rism_id}",
        "roles_sm": roles,
        "has_digital_objects_b": has_digital_objects,
        "digital_object_ids": digital_object_ids,
        "source_count_i": source_count if rism_id != "30004985" else 0,
        # "holdings_count_i": holdings_count if rism_id != "30004985" else 0,
        "total_sources_i": total_count if rism_id != "30004985" else 0,
        "work_node_ids": work_node_ids,
        "work_nodes_json": work_nodes_json,
        "works_catalogue_json": works_catalogue_json,
        "related_institutions_json": (
            orjson.dumps(related).decode("utf-8") if related else None
        ),
        "created": record["created"].strftime("%Y-%m-%dT%H:%M:%SZ"),
        "updated": record["updated"].strftime("%Y-%m-%dT%H:%M:%SZ"),
    }

    additional_fields: dict = process_marc_profile(
        person_profile, person_id, marc_record, person_processor
    )
    core_person.update(additional_fields)

    # This avoids another long lookup in the date statement processor.
    if "date_ranges_im" in core_person and isinstance(
        core_person.get("date_ranges_im"), list
    ):
        dates: list[int] = core_person["date_ranges_im"]
        core_person.update({"earliest_date_i": dates[0]})

    return core_person


def _get_work_nodes(work_nodes_marc: str, person_id: str) -> list[dict]:
    record_data = orjson.loads(work_nodes_marc)
    work_nodes = []
    for r in record_data:
        count, marc = r["count"], create_marc(r["marc_source"])
        work_node: dict | None = get_work_node(marc, person_id, "person", int(count))
        if work_node:
            work_nodes.append(work_node)

    return work_nodes


def _get_work_catalogues(work_catalogues: list) -> list | None:
    formatted_catalogues: list = []

    for catalogue in work_catalogues:
        publication_id = catalogue["id"]
        catalogue_type_value = catalogue["catalogue_type"]
        if catalogue_type_value == 3:
            catalogue_status = "completed"
        elif catalogue_type_value == 2:
            catalogue_status = "in-progress"
        else:
            catalogue_status = "unknown"

        formatted_catalogues.append(
            {
                "id": publication_id,
                "title": catalogue["title"],
                "creator": catalogue["author"],
                "short_name": catalogue["short_name"],
                "status": catalogue_status,
            }
        )

    return formatted_catalogues
