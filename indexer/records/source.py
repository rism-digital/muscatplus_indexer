import logging
from typing import Optional

import orjson
import pymarc
import yaml

from indexer.helpers.identifiers import (
    get_record_type,
    get_source_type,
    get_is_contents_record,
    get_is_collection_record,
    country_code_from_siglum,
)
from indexer.helpers.marc import create_marc, create_marc_list
from indexer.helpers.profiles import process_marc_profile
from indexer.helpers.utilities import (
    normalize_id,
    to_solr_single,
    tokenize_variants,
    get_creator_name,
    to_solr_multi,
    get_titles,
    get_content_types,
    get_parent_order_for_members,
    get_bibliographic_references_json,
    get_bibliographic_reference_titles,
)
from indexer.processors import source as source_processor
from indexer.records.holding import HoldingIndexDocument, holding_index_document
from indexer.records.incipits import get_incipits

log = logging.getLogger("muscat_indexer")
index_config: dict = yaml.full_load(open("index_config.yml", "r"))

source_profile: dict = yaml.full_load(open("profiles/sources.yml", "r"))


def create_source_index_documents(record: dict, cfg: dict) -> list:
    source: str = record["marc_source"]
    marc_record: pymarc.Record = create_marc(source)

    parent_source: Optional[str] = record.get("parent_marc_source")
    parent_marc_record: Optional[pymarc.Record] = create_marc(parent_source) if parent_source else None

    record_type_id: int = record["record_type"]
    parent_id: Optional[int] = record.get("source_id")
    child_count: int = record.get("child_count")
    # A source is always either its own member, or belonging to group of sources
    # all with the same "parent" source. This is stored in the database in the 'source_id'
    # field as either a NULL value, or the ID of the parent source.
    # If it is NULL then use the source id, indicating that it belongs to a group of 1, itself.
    # If it points to another source, use that.
    # NB: this means that a parent source will have its own ID here, while
    # all the 'children' will have a different ID. This is why the field is not called
    # 'parent_id', since it can gather all members of the group, *including* the parent.
    membership_id: int = m if (m := parent_id) else record["id"]
    rism_id: str = normalize_id(marc_record["001"].value())
    source_id: str = f"source_{rism_id}"
    num_holdings: int = record.get("holdings_count", 0)
    main_title: str = record["std_title"]

    log.debug("Indexing %s", source_id)

    creator_name: Optional[str] = get_creator_name(marc_record)
    institution_places: list[str] = (
        [s for s in record["institution_places"].split("|") if s] if record.get("institution_places") else []
    )
    source_member_composers: list[str] = (
        [s.strip() for s in record["child_composer_list"].split("\n") if s] if record.get("child_composer_list") else []
    )

    holdings_marc: list[pymarc.Record] = create_marc_list(record.get("holdings_marc"))

    all_print_holding_records: list[pymarc.Record] = []
    all_print_holding_records += holdings_marc
    all_print_holding_records += create_marc_list(record.get("parent_holdings_marc"))

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
    manuscript_holdings: list = (
        _get_manuscript_holdings(
            marc_record,
            source_id,
            main_title,
            creator_name,
            record_type_id,
            institution_places,
        )
        or []
    )
    holding_orgs: list = _get_holding_orgs(manuscript_holdings, all_print_holding_sigla) or []
    holding_orgs_ids: list = _get_holding_orgs_ids(manuscript_holdings, all_print_holding_records) or []

    holding_orgs_identifiers: list = _get_full_holding_identifiers(manuscript_holdings, all_print_holding_records) or []
    country_codes: list = _get_country_codes(manuscript_holdings, all_print_holding_records) or []

    parent_record_type_id: Optional[int] = record.get("parent_record_type")
    source_membership_data: Optional[dict] = None
    if parent_record_type_id:
        parent_material_source_types: Optional[list] = to_solr_multi(parent_marc_record, "593", "a")
        parent_material_content_types: Optional[list] = to_solr_multi(parent_marc_record, "593", "b")

        source_membership_data = {
            "source_id": f"source_{membership_id}",
            "main_title": record.get("parent_title"),
            "shelfmark": record.get("parent_shelfmark"),
            "siglum": record.get("parent_siglum"),
            "record_type": get_record_type(parent_record_type_id),
            "source_type": get_source_type(parent_record_type_id),
            "content_types_sm": get_content_types(parent_marc_record),
            "material_source_types": parent_material_source_types,
            "material_content_types": parent_material_content_types,
        }

    source_mship_json = orjson.dumps(source_membership_data).decode("utf-8") if source_membership_data else None
    source_mship_order = get_parent_order_for_members(parent_marc_record, source_id) if parent_marc_record else None

    people_names: list = list({n.strip() for n in d.split("\n") if n}) if (d := record.get("people_names")) else []
    variant_people_names: Optional[list] = _get_variant_people_names(record.get("alt_people_names"))

    source_people_ids: set[str] = (
        {f"person_{n}" for n in d.split("\n") if n} if (d := record.get("people_ids")) else set()
    )
    holding_people_ids: set[str] = _get_holding_people_ids(holdings_marc)

    # merge the two sets
    source_people_ids |= holding_people_ids
    related_people_ids: list[str] = list(source_people_ids)

    variant_standard_terms: Optional[list] = _get_variant_standard_terms(record.get("alt_standard_terms"))
    related_source_fields: list[pymarc.Field] = marc_record.get_fields("787")

    publication_entries: list = (
        list({n.strip() for n in d.split("\n") if n and n.strip()}) if (d := record.get("publication_entries")) else []
    )
    bibliographic_references: Optional[list[dict]] = get_bibliographic_references_json(
        marc_record, "691", publication_entries
    )
    bibliographic_references_json = (
        orjson.dumps(bibliographic_references).decode("utf-8") if bibliographic_references else None
    )
    bibliographic_reference_titles: Optional[list[str]] = get_bibliographic_reference_titles(publication_entries)
    works_catalogue: Optional[list[dict]] = get_bibliographic_references_json(marc_record, "690", publication_entries)
    works_catalogue_titles: Optional[list[dict]] = get_bibliographic_reference_titles(publication_entries)

    num_physical_copies: int = len(manuscript_holdings) + len(all_print_holding_records)
    has_digital_objects: bool = record.get("digital_objects") is not None
    digital_object_ids: list[str] = (
        [f"dobject_{i}" for i in record["digital_objects"].split(",") if i] if record.get("digital_objects") else []
    )

    work_ids: list = list({f"work_{n}" for n in record["work_ids"].split("\n") if n}) if record.get("work_ids") else []

    related_sources = None
    if t := record.get("related_sources"):
        related_sources = _get_related_sources(t, related_source_fields, source_id)

    related_sources_json = orjson.dumps(related_sources).decode("utf-8") if related_sources else None
    works_catalogue_json = orjson.dumps(works_catalogue).decode("utf-8") if works_catalogue else None

    related_institution_sigla = []
    if a := record.get('additional_institution_info'):
        all_institutions: list = a.split("\n") or []

        for inst in all_institutions:
            inst_components: list = inst.split("|:|")
            if len(inst_components) != 4:
                log.error("Could not parse institution entry %s", inst)
                continue

            inst_id, inst_name, relator_code, siglum = inst_components

            if siglum:
                related_institution_sigla.append(siglum)

    # add some core fields to the source. These are fields that may not be easily
    # derived directly from the MARC record, or that include data from the database.
    source_core: dict = {
        "id": source_id,
        "type": "source",
        "rism_id": rism_id,
        "source_id": source_id,
        "has_external_record_b": False,  # if the record is also in another external site (DIAMM, Cantus, etc.) then that indexer will set this to True.
        "record_type_s": get_record_type(record_type_id),
        "source_type_s": get_source_type(record_type_id),
        "content_types_sm": get_content_types(marc_record),
        # The 'source membership' fields refer to the relationship between this source and a parent record, if
        # such a relationship exists.
        "source_member_composers_sm": source_member_composers,
        "source_membership_id": f"source_{membership_id}",
        # the title of the parent record; can be NULL.
        "source_membership_title_s": record.get("parent_title"),
        "source_membership_json": source_mship_json,
        "source_membership_order_i": source_mship_order,
        # uses the std_title column in the Muscat database; cannot be NULL.
        "main_title_s": main_title,
        # Only show holding numbers for prints.
        "num_holdings_i": num_holdings if num_holdings > 0 else None,
        "num_holdings_s": _get_num_holdings_facet(num_holdings),
        "num_physical_copies_i": num_physical_copies if num_physical_copies > 0 else None,
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
        "digitization_notes_sm": _get_digitization_notes(all_marc_records),
        "has_digital_objects_b": has_digital_objects,
        "digital_object_ids": digital_object_ids,
        "bibliographic_references_json": bibliographic_references_json,
        "bibliographic_references_sm": bibliographic_reference_titles,
        "works_catalogue_sm": works_catalogue_titles,
        "work_ids": work_ids,
        "related_sources_json": related_sources_json,
        "works_catalogue_json": works_catalogue_json,
        "related_institution_sigla_sm": related_institution_sigla,
        # purposefully left empty so we can fill this up later.
        "external_records_jsonm": [],
        "created": record["created"].strftime("%Y-%m-%dT%H:%M:%SZ"),
        "updated": record["updated"].strftime("%Y-%m-%dT%H:%M:%SZ"),
    }

    # Process the MARC record and profile configuration and add additional fields
    additional_fields: dict = process_marc_profile(source_profile, source_id, marc_record, source_processor)
    source_core.update(additional_fields)

    # Extended incipits have their fingerprints calculated for similarity matching.
    # They are configurable because they slow down indexing considerably, so can be disabled
    # if faster indexing is needed.

    incipits: list = get_incipits(marc_record, source_id, main_title, record_type_id, country_codes) or []

    res: list = [source_core]
    res.extend(incipits)
    res.extend(manuscript_holdings)

    del marc_record
    del parent_marc_record
    del record

    return res


def _get_manuscript_holdings(
    record: pymarc.Record,
    source_id: str,
    main_title: str,
    creator_name: Optional[str],
    record_type_id: int,
    institution_places: list[str],
) -> Optional[list[HoldingIndexDocument]]:
    """
    Create a holding record for sources that do not actually have a holding record, e.g., manuscripts
    This is so that we can provide a unified interface for searching all holdings of an institution
    using the holding record mechanism, rather than a mixture of several different record types.
    """
    # First check to see if the record has 852 fields; if it doesn't, skip trying to process any further.
    if "852" not in record:
        return None

    holding_institution_ident: Optional[str] = to_solr_single(record, "852", "x")
    # Since these are for MSS, the holding ID is created by tying together the source id and the institution id; this
    # should result in a unique identifier for this holding record.
    holding_id: str = f"holding_{holding_institution_ident}-{source_id}"

    idx_doc: HoldingIndexDocument = holding_index_document(
        record,
        holding_id,
        source_id,
        main_title,
        creator_name,
        record_type_id,
        mss_profile=True,
    )

    # Optionally add the city if we know it.
    if len(institution_places) > 0:
        idx_doc["city_s"] = institution_places[0]

    return [idx_doc]


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


def _get_full_holding_identifiers(
    mss_holdings: list[HoldingIndexDocument], all_holdings: list[pymarc.Record]
) -> list[str]:
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
        if "856" not in record:
            continue

        digitization_links: list = [
            f
            for f in record.get_fields("856")
            if "x" in f
            and f["x"]
            in (
                "Digitalization",
                "Digitized sources",
                "Digitized",
                "IIIF",
                "IIIF manifest (digitized source)",
                "IIIF manifest (other)",
            )
        ]
        if len(digitization_links) > 0:
            return True

    return False


def _get_has_iiif_manifest(all_records: list[pymarc.Record]) -> bool:
    for record in all_records:
        if "856" not in record:
            continue

        iiif_manifests: list = [
            f
            for f in record.get_fields("856")
            if "x" in f and f["x"] in ("IIIF", "IIIF manifest (digitized source)", "IIIF manifest (other)")
        ]
        if len(iiif_manifests) > 0:
            return True

    return False


def _get_digitization_notes(all_records: list[pymarc.Record]) -> list[str]:
    all_project_notes: set = set()
    for record in all_records:
        if "856" not in record:
            continue
        recnotes: set = {f["z"] for f in record.get_fields("856") if "z" in f}
        all_project_notes |= recnotes

    return list(all_project_notes)


def _create_sigla_list_from_str(sigla: Optional[str]) -> list[str]:
    """
    Returns a list of sigla for a source. This is a set, that is cast to a list.
    Always returns a list.
    :param sigla: A string of newline-separated sigla
    :return: A list of sigla.
    """
    return list({s.strip() for s in sigla.split("\n") if s}) if sigla else []


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


def _get_related_sources(
    related: str, relationship_fields: list[pymarc.Field], host_source_id: str
) -> Optional[list[dict]]:
    """
    Combines the MARC source from related sources and the 787 entries from a record to create a JSON
    field for the related sources.

    :param related: A string containing the record IDs and MARC entries, delimited by "|~|" between the related sources
        and by "|:|" between the ID and MARC.
    :param relationship_fields: A list of 787 fields from the source MARC. Needed because this is the only place any
        notes about the relationship are stored.
    :return: A list of related sources in JSON format.
    """
    # =787  0#$nT p: Solo and Chorus ... From Cantata of "Daniel" ... Copied from the Sabbath Bell by / S[amuel] F[rederick] Van Vleck. organist / Nov. 22 1878.$w1001125501$4rdau:P60311
    notes: dict = {}

    for relfield in relationship_fields:
        sid = relfield.get("w")
        snote = relfield.get("n")
        if sid and snote:
            notes[sid] = snote

    # The related sources are first separated by "|~|" delineations between records, and then
    # "|:|" between fields in that record.
    all_records: list[str] = related.split("|~|")
    related_entries: list = []
    for relationship_id, individual_record in enumerate(all_records, 1):
        relator_code, relmarc_source = individual_record.split("|:|")
        rel_marc_record: Optional[pymarc.Record] = create_marc(relmarc_source) if relmarc_source else None
        if not rel_marc_record:
            log.error("Could not load foreign MARC record")
            continue

        record_id = normalize_id(rel_marc_record["001"].value())

        source_id: str = f"source_{record_id}"
        title: Optional[list[str]] = get_titles(rel_marc_record, "240")

        note: Optional[str] = None
        if record_id in notes:
            note = notes[record_id]

        d = {
            "id": f"{relationship_id}",
            "type": "source",
            "source_id": source_id,
            "relationship": relator_code,
            "title": title,
            "note": note,
            "this_id": host_source_id,
            "this_type": "source",
        }

        related_entries.append({k: v for k, v in d.items() if v})

    return related_entries


def _get_holding_people_ids(records: list[pymarc.Record]) -> set[str]:
    ids: set[str] = set()

    for rec in records:
        if f := to_solr_multi(rec, "700", "0"):
            p_ids: set[str] = {f"person_{i}" for i in f if i}
            ids.update(p_ids)

    return ids
