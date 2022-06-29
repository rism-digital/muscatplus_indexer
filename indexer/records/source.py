import logging
from typing import Optional

import pymarc
import ujson
import yaml

from indexer.helpers.identifiers import (
    get_record_type,
    get_source_type,
    get_content_types,
    get_is_contents_record,
    get_is_collection_record,
    country_code_from_siglum
)
from indexer.helpers.marc import create_marc
from indexer.helpers.profiles import process_marc_profile
from indexer.helpers.utilities import normalize_id, to_solr_single, tokenize_variants, get_creator_name, to_solr_multi
from indexer.processors import source as source_processor
from indexer.records.holding import HoldingIndexDocument, holding_index_document
from indexer.records.incipits import get_incipits

log = logging.getLogger("muscat_indexer")
index_config: dict = yaml.full_load(open("index_config.yml", "r"))

source_profile: dict = yaml.full_load(open('profiles/sources.yml', 'r'))


def create_source_index_documents(record: dict, cfg: dict) -> list:
    source: str = record['marc_source']
    marc_record: pymarc.Record = create_marc(source)

    parent_source: Optional[str] = record.get("parent_marc_source")
    parent_marc_record: Optional[pymarc.Record] = create_marc(parent_source) if parent_source else None

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

    creator_name: Optional[str] = get_creator_name(marc_record)
    child_record_types: list[int] = [int(s) for s in record['child_record_types'].split(",") if s] if record.get('child_record_types') else []
    parent_child_record_types: list[int] = [int(s) for s in record['parent_child_record_types'].split(",")] if record.get('parent_child_record_types') else []
    institution_places: list[str] = [s for s in record['institution_places'].split(",") if s] if record.get('institution_places') else []
    source_member_composers: list[str] = [s.strip() for s in record['child_composer_list'].split("\n") if s] if record.get('child_composer_list') else []

    all_print_holding_records: list[pymarc.Record] = []
    all_print_holding_records += _create_marc_from_str(record.get("holdings_marc"))
    all_print_holding_records += _create_marc_from_str(record.get("parent_holdings_marc"))

    all_print_holding_sigla: list[str] = []
    all_print_holding_sigla += _create_sigla_list_from_str(record.get("holdings_org"))
    all_print_holding_sigla += _create_sigla_list_from_str(record.get("parent_holdings_org"))

    all_marc_records: list[pymarc.Record] = []
    all_marc_records += [marc_record]
    all_marc_records += [parent_marc_record] if parent_marc_record else []
    all_marc_records += all_print_holding_records

    # This normalizes the holdings information to include manuscripts. This is so when a user
    # wants to see all the sources in a particular institution we can simply filter by the institution
    # id on the sources, regardless of whether they have a holding record, or they are a MS.
    manuscript_holdings: list = _get_manuscript_holdings(marc_record, source_id, main_title, creator_name, record_type_id) or []
    holding_orgs: list = _get_holding_orgs(manuscript_holdings, all_print_holding_sigla) or []
    holding_orgs_ids: list = _get_holding_orgs_ids(manuscript_holdings, all_print_holding_records) or []

    holding_orgs_identifiers: list = _get_full_holding_identifiers(manuscript_holdings, all_print_holding_records) or []
    country_codes: list = _get_country_codes(manuscript_holdings, all_print_holding_records) or []

    parent_record_type_id: Optional[int] = record.get("parent_record_type")
    source_membership_json: Optional[dict] = None
    if parent_record_type_id:
        parent_material_group_types: Optional[list] = to_solr_multi(parent_marc_record, "593", "a")

        source_membership_json = {
            "source_id": f"source_{membership_id}",
            "main_title": record.get("parent_title"),
            "shelfmark": record.get("parent_shelfmark"),
            "siglum": record.get("parent_siglum"),
            "material_types": parent_material_group_types,
            "record_type": get_record_type(parent_record_type_id),
            "source_type": get_source_type(parent_record_type_id),
            "content_types": get_content_types(parent_record_type_id, parent_child_record_types)
        }

    people_names: list = list({n.strip() for n in d.split("\n") if n}) if (d := record.get("people_names")) else []
    variant_people_names: Optional[list] = _get_variant_people_names(record.get("alt_people_names"))
    related_people_ids: list = list({f"person_{n}" for n in d.split("\n") if n}) if (d := record.get("people_ids")) else []

    variant_standard_terms: Optional[list] = _get_variant_standard_terms(record.get("alt_standard_terms"))

    publication_entries: list = list({n.strip() for n in d.split("\n") if n and n.strip()}) if (d := record.get("publication_entries")) else []
    bibliographic_references: Optional[list[dict]] = _get_bibliographic_references_json(marc_record, "691", publication_entries)
    works_catalogue: Optional[list[dict]] = _get_bibliographic_references_json(marc_record, "690", publication_entries)

    # add some core fields to the source. These are fields that may not be easily
    # derived directly from the MARC record, or that include data from the database.
    source_core: dict = {
        "id": source_id,
        "type": "source",
        "rism_id": rism_id,
        "source_id": source_id,
        "record_type_s": get_record_type(record_type_id),
        "source_type_s": get_source_type(record_type_id),
        "content_types_sm": get_content_types(record_type_id, child_record_types),

        # The 'source membership' fields refer to the relationship between this source and a parent record, if
        # such a relationship exists.
        "source_member_composers_sm": source_member_composers,
        "source_membership_id": f"source_{membership_id}",
        "source_membership_title_s": record.get("parent_title"),  # the title of the parent record; can be NULL.
        "source_membership_json": ujson.dumps(source_membership_json) if source_membership_json else None,
        "source_membership_order_i": _get_parent_order_for_members(parent_marc_record, rism_id) if parent_marc_record else None,
        "main_title_s": main_title,  # uses the std_title column in the Muscat database; cannot be NULL.
        "num_holdings_i": None if num_holdings == 0 else num_holdings,  # Only show holding numbers for prints.
        "num_holdings_s": _get_num_holdings_facet(num_holdings),
        "holding_institutions_sm": holding_orgs,
        "holding_institutions_identifiers_sm": holding_orgs_identifiers,
        "holding_institutions_ids": holding_orgs_ids,
        "holding_institutions_places_sm": institution_places,
        "country_codes_sm": country_codes,
        "people_names_sm": people_names,
        "variant_people_names_sm": variant_people_names,
        "variant_standard_terms_sm": variant_standard_terms,
        "related_people_ids": related_people_ids,
        "is_contents_record_b": get_is_contents_record(record_type_id, parent_id),
        "is_collection_record_b": get_is_collection_record(record_type_id, child_count),
        "is_composite_volume_b": record_type_id == 11,
        "has_digitization_b": _get_has_digitization(all_marc_records),
        "has_iiif_manifest_b": _get_has_iiif_manifest(all_marc_records),
        "bibliographic_references_json": ujson.dumps(bibliographic_references) if bibliographic_references else None,
        "works_catalogue_json": ujson.dumps(works_catalogue) if works_catalogue else None,
        "created": record["created"].strftime("%Y-%m-%dT%H:%M:%SZ"),
        "updated": record["updated"].strftime("%Y-%m-%dT%H:%M:%SZ")
    }

    # Process the MARC record and profile configuration and add additional fields
    additional_fields: dict = process_marc_profile(source_profile, source_id, marc_record, source_processor)
    source_core.update(additional_fields)

    # Extended incipits have their fingerprints calculated for similarity matching.
    # They are configurable because they slow down indexing considerably, so can be disabled
    # if faster indexing is needed.

    incipits: list = get_incipits(marc_record, source_id, main_title, record_type_id, child_record_types) or []

    res: list = [source_core]
    res.extend(incipits)
    res.extend(manuscript_holdings)

    del marc_record
    del parent_marc_record
    del record

    return res


def _get_manuscript_holdings(record: pymarc.Record,
                             source_id: str,
                             main_title: str,
                             creator_name: Optional[str],
                             record_type_id: int) -> Optional[list[HoldingIndexDocument]]:
    """
        Create a holding record for sources that do not actually have a holding record, e.g., manuscripts
        This is so that we can provide a unified interface for searching all holdings of an institution
        using the holding record mechanism, rather than a mixture of several different record types.
    """
    # First check to see if the record has 852 fields; if it doesn't, skip trying to process any further.
    if "852" not in record:
        return None

    source_num: str = normalize_id(record['001'].value())
    holding_institution_ident: Optional[str] = to_solr_single(record, "852", "x")
    # Since these are for MSS, the holding ID is created by tying together the source id and the institution id; this
    # should result in a unique identifier for this holding record.
    holding_id: str = f"holding_{holding_institution_ident}-{source_id}"
    holding_record_id: str = f"{holding_institution_ident}-{source_num}"

    return [holding_index_document(record, holding_id, holding_record_id, source_id, main_title, creator_name, record_type_id)]


def _get_variant_people_names(variant_names: Optional[str]) -> Optional[list]:
    if not variant_names:
        return None

    list_of_names: list = variant_names.split("\n")
    return tokenize_variants(list_of_names)


def _get_variant_standard_terms(variant_terms: Optional[str]) -> Optional[list]:
    if not variant_terms:
        return None

    list_of_terms: list = variant_terms.split("\n")
    return tokenize_variants(list_of_terms)


def _get_holding_orgs(mss_holdings: list[HoldingIndexDocument], all_holding_sigla: list[str]) -> Optional[list[str]]:
    # Coalesces both print and mss holdings into a multivalued field so that we can filter sources by their holding
    # library
    # If there are any holding records for MSS, get the siglum. Use a set to ignore any duplicates
    sigs: set[str] = set()

    for mss in mss_holdings:
        if siglum := mss.get("siglum_s"):
            sigs.add(siglum)

    for siglum in all_holding_sigla:
        sigs.add(siglum)

    return list(sigs)


def _get_holding_orgs_ids(mss_holdings: list[HoldingIndexDocument], all_holdings: list[pymarc.Record]) -> list[str]:
    ids: set[str] = set()

    for mss in mss_holdings:
        if inst_id := mss.get("institution_id"):
            ids.add(inst_id)

    for rec in all_holdings:
        if inst := to_solr_single(rec, "852", "x"):
            ids.add(f"institution_{inst}")

    return list(ids)


def _get_full_holding_identifiers(mss_holdings: list[HoldingIndexDocument], all_holdings: list[pymarc.Record]) -> list[str]:
    ids: set[str] = set()

    for mss in mss_holdings:
        institution_sig: str = mss.get("siglum_s", "")
        institution_name: str = mss.get("institution_name_s", "")
        institution_shelfmark: str = mss.get("shelfmark_s", "")
        ids.add(f"{institution_name} {institution_sig} {institution_shelfmark}")

    for rec in all_holdings:
        rec_sig: str = to_solr_single(rec, "852", "a") or ""
        rec_shelfmark: str = to_solr_single(rec, "852", "c") or ""
        rec_name: str = to_solr_single(rec, "852", "e") or ""
        ids.add(f"{rec_name} {rec_sig} {rec_shelfmark}")

    return [realid for realid in ids if realid.strip()]


def _get_country_codes(mss_holdings: list[HoldingIndexDocument], all_holdings: list[pymarc.Record]) -> list[str]:
    codes: set[str] = set()

    for mss in mss_holdings:
        institution_sig: Optional[str] = mss.get("siglum_s")
        if institution_sig:
            codes.add(country_code_from_siglum(institution_sig))

    for rec in all_holdings:
        rec_sig: Optional[str] = to_solr_single(rec, "852", "a")
        if rec_sig:
            codes.add(country_code_from_siglum(rec_sig))

    return list(codes)


def _get_has_digitization(all_records: list[pymarc.Record]) -> bool:
    """
    Looks through all records and determines whether any of them have a digitization link
    attached to them. Returns 'True' if any record is True.

    :param all_records: A list of records (source + holding) to check
    :return: A bool indicating whether any one record has the correct value in 856$x
    """
    for record in all_records:
        digitization_links: list = [f for f in record.get_fields("856") if 'x' in f and f['x'] in ("Digitalization", "Digitized sources", "Digitized", "IIIF", "IIIF manifest (digitized source)", "IIIF manifest (other)")]
        if len(digitization_links) > 0:
            return True

    return False


def _get_has_iiif_manifest(all_records: list[pymarc.Record]) -> bool:
    for record in all_records:
        iiif_manifests: list = [f for f in record.get_fields("856") if 'x' in f and f['x'] in ("IIIF", "IIIF manifest (digitized source)", "IIIF manifest (other)")]
        if len(iiif_manifests) > 0:
            return True

    return False


def _get_parent_order_for_members(parent_record: Optional[pymarc.Record], this_id: str) -> Optional[int]:
    """
    Returns an integer representing the order number of this source with respect to the order of the
    child sources listed in the parent. 0-based, since we simply look up the values in a list.

    If a child ID is not found in a parent record, or if the parent record is None, returns None.

    The form of ID being searched is normalized, so any leading zeros are stripped, etc.

    :param parent_record:
    :param this_id:
    :return:
    """
    if not parent_record:
        return None

    child_record_fields: list[pymarc.Field] = parent_record.get_fields("774")
    if not child_record_fields:
        return None

    idxs: list = []
    for field in child_record_fields:
        subf: list = field.get_subfields("w")
        if len(subf) == 0:
            continue

        subf_id = subf[0]
        if not subf_id:
            log.warning(f"Problem when searching the membership of {this_id} in {normalize_id(parent_record['001'].value())}.")
            continue

        idxs.append(normalize_id(subf_id))

    if this_id in idxs:
        return idxs.index(this_id)

    return None


def _create_sigla_list_from_str(sigla: Optional[str]) -> list[str]:
    """
    Returns a list of sigla for a source. This is a set, that is cast to a list.
    Always returns a list.
    :param sigla: A string of newline-separated sigla
    :return: A list of sigla.
    """
    return list({s.strip() for s in sigla.split("\n") if s}) if sigla else []


def _create_marc_from_str(marc_records: Optional[str]) -> list[pymarc.Record]:
    """
    Will always return a list, potentially an empty one.

    :param marc_records: A string of newline-separated MARC records
    :return: A list of pymarc.Record objects
    """
    return [create_marc(rec.strip()) for rec in marc_records.split("\n") if rec] if marc_records else []


def _get_bibliographic_references_json(record: pymarc.Record, field: str, references: Optional[list[str]]) -> Optional[list[dict]]:
    if not references:
        return None

    fields: list[pymarc.Field] = record.get_fields(field)
    if not fields:
        return None

    refs: dict = {}
    for r in references:
        # This is a unique field delimiter (hopefully!)
        rid, rest = r.split("|:| ")
        refs[rid] = rest

    outp: list = []

    for field in fields:
        fid: str = field["0"]
        literature_id: str = f"literature_{fid}"
        r = {
            "id": literature_id,
            "formatted": refs[fid],
        }
        if p := field["n"]:
            r["pages"] = p

        outp.append(r)

    return outp


def _get_num_holdings_facet(num: int) -> Optional[str]:
    if num == 0:
        return None
    elif num == 1:
        return "1"
    elif 2 <= num <= 10:
        return "2 to 10"
    elif 11 <= num <= 100:
        return "11 to 100"
    else:
        return "more than 100"
