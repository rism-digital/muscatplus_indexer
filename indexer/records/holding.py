import logging
from typing import Optional, TypedDict

import orjson
import pymarc
import yaml

from indexer.helpers.identifiers import get_record_type, get_source_type
from indexer.helpers.marc import create_marc
from indexer.helpers.profiles import process_marc_profile
from indexer.helpers.utilities import (
    get_bibliographic_references_json,
    get_content_types,
    get_creator_name,
    get_parent_order_for_members,
    to_solr_single,
)
from indexer.processors import holding as holding_processor

log = logging.getLogger("muscat_indexer")
holding_profile: dict = yaml.full_load(open("profiles/holdings.yml"))  # noqa: SIM115
mss_holding_profile: dict = yaml.full_load(open("profiles/holdingsmss.yml"))  # noqa: SIM115


class HoldingIndexDocument(TypedDict):
    id: str
    type: str
    source_id: str
    main_title_s: str
    # Convenience for URL construction; should not be used for lookups.
    holding_id_sni: str
    siglum_s: Optional[str]
    department_s: Optional[str]
    city_s: Optional[str]
    country_code_s: Optional[str]
    institution_name_s: Optional[str]
    institution_id: Optional[str]
    provenance_sm: Optional[list[str]]
    shelfmark_s: Optional[str]
    former_shelfmarks_sm: Optional[list[str]]
    material_held_sm: Optional[list[str]]
    local_numbers_sm: Optional[list[str]]
    acquisition_note_s: Optional[str]
    acquisition_date_s: Optional[str]
    acquisition_method_s: Optional[str]
    accession_number_s: Optional[str]
    access_restrictions_sm: Optional[list[str]]
    provenance_notes_sm: Optional[list[str]]
    external_resources_json: Optional[str]
    source_membership_order_i: Optional[int]
    bibliographic_references_json: Optional[str]


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

    # For consistency it's better to store the creator name with the dates attached!
    creator_name: Optional[str] = get_creator_name(source_marc_record)
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

    if composite_record := record.get("comp_marc"):
        # We can do this here since we don't need to worry about the case where a fake holding record for a MS
        # is needed. (We're indexing "real" holding records here, not making "fake" ones from the MS source record).
        composite_marc: Optional[pymarc.Record] = (
            create_marc(composite_record) if composite_record else None
        )
        (
            idx_document.update(
                {
                    "source_membership_order_i": get_parent_order_for_members(
                        composite_marc, holding_id
                    )
                    if composite_marc
                    else None
                }
            ),
        )

    if c := record.get("institution_record_marc"):
        institution_marc_record: pymarc.Record = create_marc(c)
        additional_institution_fields: Optional[dict] = (
            _index_additional_institution_fields(institution_marc_record)
        )
        idx_document.update(additional_institution_fields)

    if p := record.get("publication_entries"):
        publication_entries: list = (
            list({n.strip() for n in p.split("|~|") if n and n.strip()}) if p else []
        )
        bibliographic_references: Optional[list[dict]] = (
            get_bibliographic_references_json(marc_record, "691", publication_entries)
        )
        idx_document.update(
            {
                "bibliographic_references_json": orjson.dumps(
                    bibliographic_references
                ).decode("utf-8")
            }
        )

    return idx_document


def _index_additional_institution_fields(record: pymarc.Record) -> dict:
    ret: dict = {}

    city_field: Optional[str] = to_solr_single(record, "110", "c")
    if city_field:
        ret["city_s"] = city_field

    return ret


def holding_index_document(
    marc_record: pymarc.Record,
    holding_id: str,
    source_id: str,
    main_title: str,
    creator_name: Optional[str],
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

    holding_core: dict = {
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
            mss_holding_profile, holding_id, marc_record, holding_processor
        )
    else:
        additional_fields = process_marc_profile(
            holding_profile, holding_id, marc_record, holding_processor
        )

    holding_core.update(additional_fields)

    return holding_core
