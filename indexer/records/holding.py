import logging
from typing import TypedDict

import orjson
import pymarc
import yaml

from indexer.helpers.bibliography import get_bibliographic_references_json
from indexer.helpers.identifiers import get_record_type, get_source_type
from indexer.helpers.marc import create_marc
from indexer.helpers.profiles import compile_marc_profile, process_marc_profile
from indexer.helpers.utilities import (
    get_content_types,
    get_creator_name,
    get_parent_order_for_members,
    get_titles,
    to_solr_single,
)
from indexer.processors import holding as holding_processor

log = logging.getLogger("muscat_indexer")
raw_holding_profile: dict = yaml.full_load(open("profiles/holdings.yml"))  # noqa: SIM115
holding_profile = compile_marc_profile(raw_holding_profile, holding_processor)
raw_mss_holding_profile: dict = yaml.full_load(open("profiles/holdingsmss.yml"))  # noqa: SIM115
mss_holding_profile = compile_marc_profile(raw_mss_holding_profile, holding_processor)


class HoldingIndexDocument(TypedDict, total=False):
    id: str
    type: str
    source_id: str
    main_title_s: str
    record_type_s: str
    source_type_s: str
    content_types_sm: list[str]
    creator_name_s: str | None
    has_digital_objects_b: bool

    # Convenience for URL construction; should not be used for lookups.
    holding_id: str
    siglum_s: str | None
    department_s: str | None
    city_s: str | None
    country_code_s: str | None
    institution_name_s: str | None
    institution_id: str | None
    provenance_sm: list[str] | None
    shelfmark_s: str | None
    former_shelfmarks_sm: list[str] | None
    material_held_sm: list[str] | None
    local_numbers_sm: list[str] | None
    acquisition_note_s: str | None
    acquisition_date_s: str | None
    acquisition_method_s: str | None
    accession_number_s: str | None
    access_restrictions_sm: list[str] | None
    provenance_notes_sm: list[str] | None
    external_resources_json: str | None
    source_membership_order_i: int | None
    bibliographic_references_json: str | None
    standard_titles_json: str | None
    related_institutions_json: str | None
    digital_object_ids: list[str] | None
    created: str
    updated: str


def create_holding_index_document(record: dict, cfg: dict) -> HoldingIndexDocument:
    record_id: str = f"{record['id']}"
    membership_id: str = f"source_{record['source_id']}"
    marc_record: pymarc.Record = create_marc(record["marc_source"])
    source_marc_record: pymarc.Record = create_marc(record["source_record_marc"])

    holding_id: str = f"holding_{record_id}"
    main_title: str = record["source_title"]

    source_is_single_item: bool = (
        "774" not in source_marc_record or "773" not in source_marc_record
    )

    # For consistency, it's better to store the creator name with the dates attached!
    creator_name: str | None = get_creator_name(source_marc_record)
    record_type_id: int = record["record_type"]

    idx_document: HoldingIndexDocument = holding_index_document(
        marc_record,
        holding_id,
        membership_id,
        main_title,
        creator_name,
        record_type_id,
        source_is_single_item,
        mss_profile=False,
    )

    source_standard_title: list[dict] | None = get_titles(source_marc_record, "240")
    if source_standard_title:
        idx_document.update(
            {
                "standard_titles_json": orjson.dumps(source_standard_title).decode(
                    "utf-8"
                )
            }
        )

    if composite_record := record.get("comp_marc"):
        # We can do this here since we don't need to worry about the case where a fake holding record for a MS
        # is needed. (We're indexing "real" holding records here, not making "fake" ones from the MS source record).
        composite_marc: pymarc.Record | None = (
            create_marc(composite_record) if composite_record else None
        )

        if composite_marc:
            idx_document.update(
                {
                    "source_membership_order_i": get_parent_order_for_members(
                        composite_marc, holding_id
                    )
                }
            )

    if c := record.get("institution_record_marc"):
        institution_marc_record: pymarc.Record = create_marc(c)
        additional_institution_fields: dict = (
            _index_additional_institution_fields(institution_marc_record) or {}
        )
        idx_document.update(additional_institution_fields)

    if p := record.get("publication_entries"):
        publication_entries: list = [d for d in orjson.loads(p) if d] if p else []
        bibliographic_references: list[dict] | None = get_bibliographic_references_json(
            marc_record, "691", publication_entries
        )
        idx_document.update(
            {
                "bibliographic_references_json": orjson.dumps(
                    bibliographic_references
                ).decode("utf-8")
            }
        )

    if d := record.get("digital_objects"):
        idx_document.update(
            {
                "has_digital_objects_b": True,
                "digital_object_ids": orjson.loads(d),
            }
        )

    if "created" in record and "updated" in record:
        idx_document.update(
            {
                "created": record["created"].strftime("%Y-%m-%dT%H:%M:%SZ"),
                "updated": record["updated"].strftime("%Y-%m-%dT%H:%M:%SZ"),
            }
        )

    related: list = []
    related_institutions: list = (
        orjson.loads(ii) if (ii := record["related_institutions"]) else []
    )
    for i, reli in enumerate(related_institutions, 1):
        institution_record: dict = {
            "id": f"{i}",
            "institution_id": f"{reli['institution_id']}",
            "type": "institution",
            "name": f"{reli['name']}",
            "city": f"{reli['place']}",
            "siglum": reli["siglum"],
            "relationship": reli["relator"],
            "this_id": holding_id,
            "this_type": "holding",
        }
        related.append({k: v for k, v in institution_record.items() if v})

    if related:
        idx_document.update(
            {"related_institutions_json": orjson.dumps(related).decode("utf-8")}
        )

    return idx_document


def _index_additional_institution_fields(record: pymarc.Record) -> dict:
    ret: dict = {}

    city_field: str | None = to_solr_single(record, "110", "c")
    if city_field:
        ret["city_s"] = city_field

    return ret


def holding_index_document(
    marc_record: pymarc.Record,
    holding_id: str,
    source_id: str,
    main_title: str,
    creator_name: str | None,
    record_type_id: int,
    source_single_item: bool,
    mss_profile: bool,
) -> HoldingIndexDocument:
    """
    The holding index documents are used for indexing BOTH holding records AND source records for manuscripts. In this
    way we can ensure that the structure of the index is the same for both of these types of holdings.

    :param marc_record: A pymarc holding record instance
    :param holding_id: The holding record ID. In the case of MSS this is composed of the institution and source ids.
    :param source_id: The id of the parent record; if no parent record, this is the same as the record_id.
    :param main_title: The main title of the source record. Used primarily for link text, etc.
    :param creator_name: The name of the composer / author of the source. This is stored primarily for display.
    :param record_type_id: The value of the record type identifier from the Muscat DB
    :param source_single_item: An indicator of whether the source record is a "single item" -- no parents, no children.
    :param mss_profile: Whether to use the Manuscripts profile ('holdingsmss.yml') for creating an exemplar record.
    :return: A holding index document.
    """
    if "-" in holding_id:
        holding_id_alone, _ = holding_id.split("-")
    else:
        holding_id_alone = holding_id

    holding_core: HoldingIndexDocument = {
        "id": holding_id,
        "type": "holding",
        "source_id": source_id,
        "holding_id": holding_id_alone,
        "record_type_s": get_record_type(record_type_id, source_single_item),
        "source_type_s": get_source_type(record_type_id),
        "content_types_sm": get_content_types(marc_record),
        "main_title_s": main_title,
        "creator_name_s": creator_name,
    }

    if mss_profile:
        additional_fields = process_marc_profile(
            mss_holding_profile, holding_id, marc_record
        )
    else:
        additional_fields = process_marc_profile(
            holding_profile, holding_id, marc_record
        )

    holding_core.update(additional_fields)

    return holding_core
