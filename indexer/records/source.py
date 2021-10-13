import logging
from typing import List, Dict, Optional

import pymarc
import ujson
import yaml

from indexer.helpers.identifiers import RecordTypes
from indexer.helpers.marc import create_marc
from indexer.helpers.profiles import process_marc_profile
from indexer.helpers.utilities import normalize_id, to_solr_single, tokenize_variants
from indexer.processors import source as source_processor
from indexer.records.holding import HoldingIndexDocument, holding_index_document
from indexer.records.incipits import get_incipits

log = logging.getLogger("muscat_indexer")
index_config: Dict = yaml.full_load(open("index_config.yml", "r"))

source_profile: Dict = yaml.full_load(open('profiles/sources.yml', 'r'))


def create_source_index_documents(record: Dict) -> List:
    source: str = record['marc_source']
    marc_record: pymarc.Record = create_marc(source)

    record_type_id: int = record['record_type']
    parent_id: Optional[int] = record.get('source_id')
    child_count: int = record.get("child_count")
    # A source is always either its own member, or belonging to group of sources
    # all with the same "parent" source. This is stored in the database in the 'source_id'
    # field as either a NULL value, or the ID of the parent source.
    # If it is NULL then use the source id, indicating that it belongs to a group of 1, itself.
    # If it points to another source, use that.
    # NB: this means that a parent source will have its own ID here, while
    # all the 'children' will have a different ID. This is why the field is not called
    # 'parent_id', since it can gather all members of the group, *including* the parent.
    membership_id: int = m if (m := parent_id) else record['id']
    rism_id: str = normalize_id(marc_record['001'].value())
    source_id: str = f"source_{rism_id}"
    num_holdings: int = record.get("holdings_count")
    main_title: str = record['std_title']
    child_record_types: list[int] = [int(s) for s in record['child_record_types'].split(",")] if record['child_record_types'] else []

    # This normalizes the holdings information to include manuscripts. This is so when a user
    # wants to see all the sources in a particular institution we can simply filter by the institution
    # id on the sources, regardless of whether they have a holding record, or they are a MS.
    manuscript_holdings: List = _get_manuscript_holdings(marc_record, source_id, main_title) or []
    holding_orgs: List = _get_holding_orgs(manuscript_holdings, record.get("holdings_org"), record.get("parent_holdings_org")) or []
    holding_orgs_ids: List = _get_holding_orgs_ids(manuscript_holdings, record.get("holdings_marc"), record.get("parent_holdings_marc")) or []
    holding_orgs_identifiers: List = _get_full_holding_identifiers(manuscript_holdings, record.get("holdings_marc"), record.get("parent_holdings_marc")) or []

    parent_record_type_id: Optional[int] = record.get("parent_record_type")
    source_membership_json: Optional[Dict] = None
    if parent_record_type_id:
        source_membership_json = {
            "source_id": f"source_{membership_id}",
            "main_title": record.get("parent_title"),
        }

    people_names: List = list({n.strip() for n in d.split("\n") if n}) if (d := record.get("people_names")) else []
    variant_people_names: Optional[List] = _get_variant_people_names(record.get("alt_people_names"))
    related_people_ids: List = list({f"person_{n}" for n in d.split("\n") if n}) if (d := record.get("people_ids")) else []

    variant_standard_terms: Optional[list] = _get_variant_standard_terms(record.get("alt_standard_terms"))

    # add some core fields to the source. These are fields that may not be easily
    # derived directly from the MARC record, or that include data from the database.
    source_core: Dict = {
        "id": source_id,
        "type": "source",
        "rism_id": rism_id,
        "source_id": source_id,
        "source_type_s": _get_source_type(record_type_id),
        "content_types_sm": _get_content_type(record_type_id, child_record_types),
        "source_membership_id": f"source_{membership_id}",
        "source_membership_title_s": record.get("parent_title"),  # the title of the parent record; can be NULL.
        "source_membership_json": ujson.dumps(source_membership_json) if source_membership_json else None,
        "main_title_s": main_title,  # uses the std_title column in the Muscat database; cannot be NULL.
        "num_holdings_i": 1 if num_holdings == 0 else num_holdings,  # every source has at least one exemplar
        "holding_institutions_sm": holding_orgs,
        "holding_institutions_identifiers_sm": holding_orgs_identifiers,
        "holding_institutions_ids": holding_orgs_ids,
        "people_names_sm": people_names,
        "variant_people_names_sm": variant_people_names,
        "variant_standard_terms_sm": variant_standard_terms,
        "related_people_ids": related_people_ids,
        "is_contents_record_b": _get_is_contents_record(record_type_id, parent_id),
        "is_collection_record_b": _get_is_collection_record(record_type_id, child_count),
        "is_composite_volume_b": record_type_id == 11,
        "created": record["created"].strftime("%Y-%m-%dT%H:%M:%SZ"),
        "updated": record["updated"].strftime("%Y-%m-%dT%H:%M:%SZ")
    }

    # Process the MARC record and profile configuration and add additional fields
    additional_fields: Dict = process_marc_profile(source_profile, source_id, marc_record, source_processor)
    source_core.update(additional_fields)

    # Extended incipits have their fingerprints calculated for similarity matching.
    # They are configurable because they slow down indexing considerably, so can be disabled
    # if faster indexing is needed.
    incipits: List = get_incipits(marc_record, source_id, main_title) or []

    res: List = [source_core]
    res.extend(incipits)
    res.extend(manuscript_holdings)

    return res


def _get_source_type(record_type_id: int) -> str:
    if record_type_id in (
        RecordTypes.EDITION,
        RecordTypes.EDITION_CONTENT,
        RecordTypes.LIBRETTO_EDITION,
        RecordTypes.THEORETICA_EDITION,
        RecordTypes.LIBRETTO_EDITION_CONTENT,
        RecordTypes.THEORETICA_EDITION_CONTENT
    ):
        return "printed"
    elif record_type_id in (
        RecordTypes.COLLECTION,
        RecordTypes.SOURCE,
        RecordTypes.LIBRETTO_SOURCE,
        RecordTypes.THEORETICA_SOURCE
    ):
        return "manuscript"
    elif record_type_id in (
        RecordTypes.COMPOSITE_VOLUME,
    ):
        return "composite"
    else:
        return "unspecified"


def _get_content_type(record_type_id: int, child_record_types: list[int]) -> List[str]:
    """
    Takes all record types associated with this record, and returns a list of
    all possible content types for it.

    Checks if two sets have an intersection set (that they have members overlapping).

    :param record_type_id: The record type id of the source record
    :param child_record_types: The record type ids of all child records
    :return: A list of index values containing the content types.
    """
    all_types: set = set([record_type_id] + child_record_types)
    ret: list = []

    if all_types & {RecordTypes.LIBRETTO_EDITION_CONTENT,
                    RecordTypes.LIBRETTO_EDITION,
                    RecordTypes.LIBRETTO_SOURCE}:
        ret.append("libretto")

    if all_types & {RecordTypes.THEORETICA_EDITION_CONTENT,
                    RecordTypes.THEORETICA_EDITION,
                    RecordTypes.THEORETICA_SOURCE}:
        ret.append("treatise")

    if all_types & {RecordTypes.SOURCE,
                    RecordTypes.EDITION,
                    RecordTypes.EDITION_CONTENT}:
        ret.append("musical_source")

    return ret


def _get_is_contents_record(record_type_id: int, parent_id: Optional[int]) -> bool:
    if record_type_id in (
        RecordTypes.EDITION_CONTENT,
        RecordTypes.LIBRETTO_EDITION_CONTENT,
        RecordTypes.THEORETICA_EDITION_CONTENT
    ):
        return True
    elif record_type_id in (
        RecordTypes.SOURCE,
        RecordTypes.LIBRETTO_SOURCE,
        RecordTypes.THEORETICA_SOURCE
    ) and parent_id is not None:
        return True
    else:
        return False


def _get_is_collection_record(record_type_id: int, children_count: int) -> bool:
    if record_type_id in (
        RecordTypes.COLLECTION,
        RecordTypes.LIBRETTO_SOURCE,
        RecordTypes.LIBRETTO_EDITION,
        RecordTypes.THEORETICA_SOURCE,
        RecordTypes.THEORETICA_EDITION
    ) and children_count > 0:
        return True
    return False


def _get_manuscript_holdings(record: pymarc.Record, source_id: str, main_title: str) -> Optional[List[HoldingIndexDocument]]:
    """
        Create a holding record for sources that do not actually have a holding record, e.g., manuscripts
        This is so that we can provide a unified interface for searching all holdings of an institution
        using the holding record mechanism, rather than a mixture of several different record types.
    """
    # First check to see if the record has 852 fields; if it doesn't, skip trying to process any further.
    if "852" not in record:
        return None

    source_num: str = record['001'].value()
    holding_institution_ident: Optional[str] = to_solr_single(record, "852", "x")
    # Since these are for MSS, the holding ID is created by tying together the source id and the institution id; this
    # should result in a unique identifier for this holding record.
    holding_id: str = f"holding_{holding_institution_ident}-{source_id}"
    holding_record_id: str = f"{holding_institution_ident}-{source_num}"

    return [holding_index_document(record, holding_id, holding_record_id, source_id, main_title)]


def _get_variant_people_names(variant_names: Optional[str]) -> Optional[List]:
    if not variant_names:
        return None

    list_of_names: List = variant_names.split("\n")
    return tokenize_variants(list_of_names)


def _get_variant_standard_terms(variant_terms: Optional[str]) -> Optional[list]:
    if not variant_terms:
        return None

    list_of_terms: list = variant_terms.split("\n")
    return tokenize_variants(list_of_terms)


def _get_holding_orgs(mss_holdings: List[HoldingIndexDocument], print_holdings: Optional[str] = None, parent_holdings: Optional[str] = None) -> Optional[List[str]]:
    # Coalesces both print and mss holdings into a multivalued field so that we can filter sources by their holding
    # library
    # If there are any holding records for MSS, get the siglum. Use a set to ignore any duplicates
    sigs: set[str] = set()

    for mss in mss_holdings:
        if siglum := mss.get("siglum_s"):
            sigs.add(siglum)

    all_holdings: List = []

    if print_holdings:
        all_holdings += print_holdings.split("\n")

    if parent_holdings:
        all_holdings += parent_holdings.split("\n")

    for lib in all_holdings:
        if siglum := lib.strip():
            sigs.add(siglum)

    return list(sigs)


def _get_holding_orgs_ids(mss_holdings: List[HoldingIndexDocument], print_holdings: Optional[str] = None, parent_holdings: Optional[str] = None) -> List[str]:
    ids: set[str] = set()

    for mss in mss_holdings:
        if inst_id := mss.get("institution_id"):
            ids.add(inst_id)

    all_marc_records: List = []

    if print_holdings:
        all_marc_records += print_holdings.split("\n")

    if parent_holdings:
        all_marc_records += parent_holdings.split("\n")

    for rec in all_marc_records:
        rec = rec.strip()
        m: pymarc.Record = create_marc(rec)

        if inst := to_solr_single(m, "852", "x"):
            ids.add(f"institution_{inst}")

    return list(ids)


def _get_full_holding_identifiers(mss_holdings: List[HoldingIndexDocument], print_holdings: Optional[str] = None, parent_holdings: Optional[str] = None) -> List[str]:
    ids: set[str] = set()

    for mss in mss_holdings:
        institution_sig: str = mss.get("siglum_s", "")
        institution_name: str = mss.get("institution_s", "")
        institution_shelfmark: str = mss.get("shelfmark_s", "")
        ids.add(f"{institution_name} {institution_sig} {institution_shelfmark}")

    all_marc_records: List = []

    if print_holdings:
        all_marc_records += print_holdings.split("\n")
    if parent_holdings:
        all_marc_records += parent_holdings.split("\n")

    for rec in all_marc_records:
        rec = rec.strip()
        m: pymarc.Record = create_marc(rec)

        rec_sig: str = to_solr_single(m, "852", "a") or ""
        rec_shelfmark: str = to_solr_single(m, "852", "c") or ""
        rec_name: str = to_solr_single(m, "852", "e") or ""
        ids.add(f"{rec_name} {rec_sig} {rec_shelfmark}")

    return [realid for realid in ids if realid.strip()]
