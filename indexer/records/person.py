import logging
from typing import Optional, TypedDict

import pymarc
import yaml

from indexer.helpers.marc import create_marc
from indexer.helpers.profiles import process_marc_profile
from indexer.helpers.utilities import normalize_id
from indexer.processors import person as person_processor

log = logging.getLogger("muscat_indexer")
person_profile: dict = yaml.full_load(open("profiles/people.yml"))  # noqa: SIM115


class PersonIndexDocument(TypedDict):
    id: str
    type: str
    person_id: str
    name_s: Optional[str]
    date_statement_s: Optional[str]
    other_dates_s: Optional[str]
    variant_names_sm: Optional[list]
    related_places_sm: Optional[list]
    related_people_sm: Optional[list]
    related_institutions_sm: Optional[list]
    general_notes_sm: Optional[list]
    additional_biography_sm: Optional[list]
    gender_s: Optional[str]
    roles_sm: Optional[list]
    external_ids: Optional[list]
    boost: int
    related_people_json: Optional[str]
    related_places_json: Optional[str]
    related_institutions_json: Optional[str]
    name_variants_json: Optional[str]
    external_resources_json: Optional[str]


def create_person_index_document(record: dict, cfg: dict) -> dict:
    marc_record: pymarc.Record = create_marc(record["marc_source"])
    rism_id: str = normalize_id(marc_record["001"].value())
    person_id: str = f"person_{rism_id}"
    roles: list[str] = (
        [s.strip() for s in record["source_relationships"].split(",") if s]
        if record.get("source_relationships")
        else []
    )

    source_count: int = record.get("source_count", 0)
    holdings_count: int = record.get("holdings_count", 0)
    total_count: int = source_count + holdings_count
    has_digital_objects: bool = record.get("digital_objects") is not None
    digital_object_ids: list[str] = (
        [f"dobject_{i}" for i in record["digital_objects"].split(",") if i]
        if record.get("digital_objects")
        else []
    )

    # For the source count we take the literal count *except* for the Anonymous user,
    # since that throws everything off.
    core_person: dict = {
        "type": "person",
        "id": person_id,
        "person_id": person_id,
        "rism_id": rism_id,
        "roles_sm": roles,
        "has_digital_objects_b": has_digital_objects,
        "digital_object_ids": digital_object_ids,
        "source_count_i": source_count if rism_id != "30004985" else 0,
        # "holdings_count_i": holdings_count if rism_id != "30004985" else 0,
        "total_sources_i": total_count if rism_id != "30004985" else 0,
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
