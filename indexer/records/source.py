import logging
from typing import Any

import orjson
import pymarc
import yaml

from indexer.helpers.bibliography import (
    get_bibliographic_reference_titles,
    get_bibliographic_references_json,
)
from indexer.helpers.identifiers import (
    country_code_from_siglum,
    get_is_collection_record,
    get_is_contents_record,
    get_record_type,
    get_source_type,
)
from indexer.helpers.marc import create_marc, create_marc_list
from indexer.helpers.profiles import process_marc_profile
from indexer.helpers.utilities import (
    get_content_types,
    get_creator_name,
    get_parent_order_for_members,
    get_people_names,
    get_related_sources,
    get_work_node,
    to_solr_multi,
    to_solr_single,
    tokenize_variants,
)
from indexer.processors import source as source_processor
from indexer.records.holding import HoldingIndexDocument, holding_index_document
from indexer.records.incipits import get_source_incipits

log = logging.getLogger("muscat_indexer")
with open("index_config.yml") as idx_cf:
    index_config: dict = yaml.full_load(idx_cf)

with open("profiles/sources.yml") as source_pf:
    source_profile: dict = yaml.full_load(source_pf)


def create_source_index_documents(record: dict, cfg: dict) -> list[dict]:
    source: str = record["marc_source"]
    marc_record: pymarc.Record = create_marc(source)

    parent_source: str | None = record.get("parent_marc_source")
    parent_marc_record: pymarc.Record | None = (
        create_marc(parent_source) if parent_source else None
    )

    record_type_id: int = record["record_type"]
    parent_id: int | None = record.get("source_id")
    child_count: int = record.get("child_count", 0)
    # A source is always either its own member, or belonging to group of sources
    # all with the same "parent" source. This is stored in the Muscat database in the 'source_id'
    # field as either a NULL value, or the ID of the parent source.
    # If it is a NULL value then use the source id, indicating that it belongs to a group of 1, itself.
    # If it points to another source, use that.
    # NB: this means that a parent source will have its own ID here, while
    # all the 'children' will have a different ID. This is why the field is not called
    # 'parent_id', since it can gather all members of the group, *including* the parent.
    membership_id: int = m if (m := parent_id) else record["id"]
    rism_id: str = marc_record["001"].value()
    source_id: str = f"source_{rism_id}"
    child_holdings_count: int = record.get("child_holdings_count", 0)
    parent_holdings_count: int = record.get("parent_holdings_count", 0)
    num_holdings: int = child_holdings_count + parent_holdings_count
    main_title: str = record["std_title"]

    # If a source has no parent, and no children, then it is a single-item source.
    is_single_item: bool = parent_id is None and child_count == 0

    log.debug("Indexing %s", source_id)
    child_marc: list = (
        orjson.loads(ch) if (ch := record.get("child_marc_records")) else []
    )
    child_marc_records: list[pymarc.Record] = create_marc_list(child_marc)
    child_composers: set[str] = set()
    for child_record in child_marc_records:
        c: str | None = get_creator_name(child_record)
        if not c:
            continue
        child_composers.add(c)

    creator_name: str | None = get_creator_name(marc_record)

    holding_info: list[dict] = orjson.loads(m) if (m := record.get("holdings")) else []
    parent_holding_info: list[dict] = (
        orjson.loads(mp) if (mp := record.get("parent_holdings_marc")) else []
    )

    holdings_marc: list[pymarc.Record] = create_marc_list(
        [m["marc_source"] for m in holding_info]
    )

    all_print_holding_records: list[pymarc.Record] = []
    all_print_holding_records += holdings_marc
    all_print_holding_records += create_marc_list(
        [pm["marc_source"] for pm in parent_holding_info]
    )

    print_holding_relationships: list[str] = (
        _get_print_holding_institution_relationships(all_print_holding_records)
    )

    all_print_holding_sigla: list[str] = []
    all_print_holding_sigla += [m["lib_siglum"] for m in holding_info]
    all_print_holding_sigla += [pm["lib_siglum"] for pm in parent_holding_info]

    all_marc_records: list[pymarc.Record] = []
    all_marc_records += [marc_record]
    all_marc_records += [parent_marc_record] if parent_marc_record else []
    all_marc_records += all_print_holding_records

    related_institution_sigla = []
    institution_places: list = []
    ms_holding_institutions: list = (
        orjson.loads(a) if (a := record.get("ms_holding_institutions")) else []
    )
    for inst in ms_holding_institutions:
        if s := inst.get("siglum"):
            related_institution_sigla.append(s)
        if p := inst.get("place"):
            institution_places.append(p)

    # This normalizes the holdings information to include manuscripts. This is so when a user
    # wants to see all the sources in a particular institution we can simply filter by the institution
    # id on the sources, regardless of whether they have a holding record, or they are a MS.
    manuscript_holdings: list[HoldingIndexDocument] = (
        _get_manuscript_holdings(
            marc_record,
            source_id,
            is_single_item,
            main_title,
            creator_name,
            record_type_id,
            institution_places,
        )
        or []
    )
    holding_orgs: list = (
        _get_holding_orgs(manuscript_holdings, all_print_holding_sigla) or []
    )
    holding_orgs_ids: list = (
        _get_holding_orgs_ids(manuscript_holdings, all_print_holding_records) or []
    )

    holding_orgs_identifiers: list = (
        _get_full_holding_identifiers(manuscript_holdings, all_print_holding_records)
        or []
    )
    country_codes: list = (
        _get_country_codes(manuscript_holdings, all_print_holding_records) or []
    )

    source_membership_data: dict | None = None
    if parent_record_type_id := record["parent_record_type"]:
        parent_material_source_types: list[str] | None = to_solr_multi(
            parent_marc_record, "593", "a"
        )
        parent_material_content_types: list[str] | None = to_solr_multi(
            parent_marc_record, "593", "b"
        )

        source_membership_data = {
            "source_id": f"source_{membership_id}",
            "main_title": record.get("parent_title"),
            "shelfmark": record.get("parent_shelfmark"),
            "siglum": record.get("parent_siglum"),
            # If a child has a parent, then by definition we do not have a single item.
            "record_type": get_record_type(parent_record_type_id, False),
            "source_type": get_source_type(parent_record_type_id),
            "content_types_sm": get_content_types(parent_marc_record),
            "material_source_types": parent_material_source_types,
            "material_content_types": parent_material_content_types,
        }

    source_mship_json = (
        orjson.dumps(source_membership_data).decode("utf-8")
        if source_membership_data
        else None
    )
    source_mship_order = (
        get_parent_order_for_members(parent_marc_record, source_id)
        if parent_marc_record
        else None
    )

    people: list = orjson.loads(d) if (d := record.get("people")) else []
    people_names: list | None = get_people_names(people)

    variant_people_names: list | None = _get_variant_people_names(people)

    source_people_ids: set[str] = {p["id"] for p in people}
    holding_people_ids: set[str] = _get_holding_people_ids(holdings_marc)

    # merge the two sets
    source_people_ids |= holding_people_ids
    related_people_ids: list[str] = list(source_people_ids)

    variant_standard_terms: list | None = _get_variant_standard_terms(
        orjson.loads(st) if (st := record.get("alt_standard_terms")) else []
    )
    related_source_fields: list[pymarc.Field] = marc_record.get_fields("787")

    publication_entries: list = (
        [p for p in orjson.loads(d) if p]
        if (d := record.get("publication_entries"))
        else []
    )
    bibliographic_references: list[dict] | None = get_bibliographic_references_json(
        marc_record, "691", publication_entries
    )
    bibliographic_references_json = (
        orjson.dumps(bibliographic_references).decode("utf-8")
        if bibliographic_references
        else []
    )
    bibliographic_reference_titles: list[str] | None = (
        get_bibliographic_reference_titles(publication_entries)
    )
    works_catalogue: list[dict] | None = get_bibliographic_references_json(
        marc_record, "690", publication_entries
    )
    works_catalogue_titles: list[str] | None = get_bibliographic_reference_titles(
        publication_entries
    )

    num_physical_copies: int = len(manuscript_holdings) + len(all_print_holding_records)
    has_digital_objects: bool = record.get("digital_objects") is not None
    digital_object_ids: list[str] = (
        orjson.loads(d) if (d := record.get("digital_objects")) else []
    )

    work_ids: list = orjson.loads(d) if (d := record.get("work_ids")) else []

    related_sources = None
    if t := record.get("related_sources"):
        source_list: list = orjson.loads(t)
        related_sources = get_related_sources(
            source_list, related_source_fields, source_id
        )

    related_sources_json = (
        orjson.dumps(related_sources).decode("utf-8") if related_sources else None
    )
    works_catalogue_json = (
        orjson.dumps(works_catalogue).decode("utf-8") if works_catalogue else None
    )

    has_digitization: bool = _get_has_digitization(all_marc_records)

    work_node_json = None
    work_node_id = None
    if w := record.get("work_node"):
        wndata = orjson.loads(w)
        wn_record: pymarc.Record = create_marc(wndata["marc_source"])
        work_node: dict | None = get_work_node(wn_record, source_id, "source")
        if work_node and "external_id" in work_node:
            work_node_id = work_node["external_id"]
            work_node_json = orjson.dumps(work_node).decode("utf-8")

    locations_peformances_db: list = (
        orjson.loads(ii) if (ii := record.get("locations_of_performances")) else []
    )

    locations_peformances: list = [
        {k: v for k, v in it.items() if v} for it in locations_peformances_db
    ]
    locations_peformances_json: str | None = (
        orjson.dumps(locations_peformances).decode("utf-8")
        if locations_peformances
        else None
    )

    # add some core fields to the source. These are fields that may not be easily
    # derived directly from the MARC record, or that include data from the database.
    source_core: dict = {
        "id": source_id,
        "type": "source",
        "rism_id": rism_id,
        "full_rism_id": f"sources/{rism_id}",
        "source_id": source_id,
        "has_external_record_b": False,
        # if the record is also in another external site (DIAMM, Cantus, etc.) then that indexer will set this to True.
        "record_type_s": get_record_type(record_type_id, is_single_item),
        "source_type_s": get_source_type(record_type_id),
        "content_types_sm": get_content_types(marc_record),
        # The 'source membership' fields refer to the relationship between this source and a parent record, if
        # such a relationship exists.
        "source_member_composers_sm": list(child_composers),
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
        "num_physical_copies_i": num_physical_copies
        if num_physical_copies > 0
        else None,
        "holding_institutions_sm": holding_orgs,
        "holding_institutions_identifiers_sm": holding_orgs_identifiers,
        "holding_institutions_ids": holding_orgs_ids,
        "holding_institutions_places_sm": institution_places,
        "holding_institutions_relationships_ids": print_holding_relationships,
        "holding_material_held_sm": _get_print_holdings_material_held(
            all_print_holding_records
        ),
        "country_codes_sm": country_codes,
        "people_names_sm": people_names,
        "variant_people_names_sm": variant_people_names,
        "variant_standard_terms_sm": variant_standard_terms,
        "related_people_ids": related_people_ids,
        "is_contents_record_b": get_is_contents_record(record_type_id, parent_id),
        "is_collection_record_b": get_is_collection_record(record_type_id, child_count),
        "is_composite_volume_b": record_type_id == 11,
        "has_digitization_b": has_digitization,
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
        "location_of_performance_json": locations_peformances_json,
        "related_institution_sigla_sm": related_institution_sigla,
        "work_node_json": work_node_json,
        "work_node_id": work_node_id,
        # purposefully left empty so we can fill this up later.
        "external_records_jsonm": [],
        "created": record["created"].strftime("%Y-%m-%dT%H:%M:%SZ"),
        "updated": record["updated"].strftime("%Y-%m-%dT%H:%M:%SZ"),
    }

    # Process the MARC record and profile configuration and add additional fields
    additional_fields: dict = process_marc_profile(
        source_profile, source_id, marc_record, source_processor
    )
    source_core.update(additional_fields)

    # Combine all the institution ids into a single list for an easier lookup.
    related_institution_ids: list[str] = additional_fields.get(
        "related_institutions_ids", []
    )

    # 710 (ms), 852 (pr & ms), 710 (holding for pr.)
    all_related_institutions: list[str] = (
        related_institution_ids + holding_orgs_ids + print_holding_relationships
    )
    # remove duplicates with a set.
    # NB: This field should only be used on sources, since we will dynamically calculate the
    # number of related institutions based on the number of occurrences of that institution id
    # in this field.
    source_core["all_related_institutions_ids"] = list(set(all_related_institutions))
    creator_id: list[str] = [f] if (f := additional_fields.get("creator_id")) else []
    source_core["all_related_people_ids"] = list(set(related_people_ids + creator_id))

    source_dates: list[int] | None = additional_fields.get("date_ranges_im")
    creator_name = additional_fields.get("creator_name_s")

    # Extended incipits have their fingerprints calculated for similarity matching.
    # They are configurable because they slow down indexing considerably, so can be disabled
    # if faster indexing is needed.

    incipits: list = (
        get_source_incipits(
            marc_record,
            main_title,
            record_type_id,
            country_codes,
            has_digitization,
            creator_name,
            source_dates,
        )
        or []
    )

    res: list = [source_core]
    res.extend(incipits)
    res.extend(manuscript_holdings)

    del marc_record
    del parent_marc_record
    del record
    del child_marc_records

    return res


def _get_manuscript_holdings(
    record: pymarc.Record,
    source_id: str,
    source_is_single_item: bool,
    main_title: str,
    creator_name: str | None,
    record_type_id: int,
    institution_places: list[str],
) -> list[HoldingIndexDocument] | None:
    """
    Create a holding record for sources that do not actually have a holding record, e.g., manuscripts
    This is so that we can provide a unified interface for searching all holdings of an institution
    using the holding record mechanism, rather than a mixture of several different record types.
    """
    # First check to see if the record has 852 fields; if it doesn't, skip trying to process any further.
    if "852" not in record:
        return None

    holding_institution_ident: str | None = to_solr_single(record, "852", "x")
    # Since these are for MSS, the holding ID is created by tying together the source id and the institution id; this
    # should result in a unique identifier for this holding record.
    holding_id: str = f"holding_{holding_institution_ident}-{source_id}"

    idx_doc: dict[str, Any] = holding_index_document(
        record,
        holding_id,
        source_id,
        main_title,
        creator_name,
        record_type_id,
        source_is_single_item,
        mss_profile=True,
    )

    # Optionally add the city if we know it.
    if len(institution_places) > 0:
        idx_doc["city_s"] = institution_places[0]

    return [idx_doc]


def _get_variant_people_names(people: list | None) -> list | None:
    if not people:
        return None

    variant_names: list[str] = [
        v for p in people for v in p["alternate_names"].splitlines() if p
    ]
    return tokenize_variants(variant_names)


def _get_variant_standard_terms(variant_terms: list | None) -> list | None:
    if not variant_terms:
        return None

    list_of_terms: list[str] = [t for ts in variant_terms for t in ts.splitlines() if t]
    return tokenize_variants(list_of_terms)


def _get_holding_orgs(
    mss_holdings: list[HoldingIndexDocument], all_holding_sigla: list[str]
) -> list[str] | None:
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


def _get_holding_orgs_ids(
    mss_holdings: list[HoldingIndexDocument], all_holdings: list[pymarc.Record]
) -> list[str]:
    ids: set[str] = set()

    for mss in mss_holdings:
        if inst_id := mss.get("institution_id"):
            ids.add(inst_id)

    for rec in all_holdings:
        if inst := to_solr_single(rec, "852", "x"):
            ids.add(f"institution_{inst}")

    return list(ids)


def _get_full_holding_identifiers(
    mss_holdings: list[dict[str, str]], all_holdings: list[pymarc.Record]
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


def _get_country_codes(
    mss_holdings: list[HoldingIndexDocument], all_holdings: list[pymarc.Record]
) -> list[str]:
    codes: set[str] = set()

    for mss in mss_holdings:
        institution_sig: str | None = mss.get("siglum_s")
        if institution_sig:
            codes.add(country_code_from_siglum(institution_sig))

    for rec in all_holdings:
        rec_sig: str | None = to_solr_single(rec, "852", "a")
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
    digitized_labels = (
        "Digitalization",
        "Digitized sources",
        "Digitized",
        "IIIF",
        "IIIF manifest (digitized source)",
        "IIIF manifest (other)",
    )

    for record in all_records:
        if "856" not in record:
            continue

        for f in record.get_fields("856"):
            if "x" not in f:
                continue
            if f["x"] in digitized_labels:
                return True

    return False


def _get_has_iiif_manifest(all_records: list[pymarc.Record]) -> bool:
    iiif_labels = ("IIIF", "IIIF manifest (digitized source)", "IIIF manifest (other)")

    for record in all_records:
        if "856" not in record:
            continue

        for f in record.get_fields("856"):
            if "x" not in f:
                continue
            if f["x"] in iiif_labels:
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


def _get_num_holdings_facet(num: int) -> str | None:
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


def _get_holding_people_ids(records: list[pymarc.Record]) -> set[str]:
    ids: set[str] = set()

    for rec in records:
        if f := to_solr_multi(rec, "700", "0"):
            p_ids: set[str] = {f"person_{i}" for i in f if i}
            ids.update(p_ids)

    return ids


def _get_print_holding_institution_relationships(
    print_records: list[pymarc.Record],
) -> list[str]:
    ids: set[str] = set()

    for rec in print_records:
        if f := to_solr_multi(rec, "710", "0"):
            i_ids: set[str] = {f"institution_{i}" for i in f if i}
            ids.update(i_ids)
    return list(ids)


def _get_print_holdings_material_held(
    print_records: list[pymarc.Record],
) -> list[str] | None:
    material_held: set[str] = set()

    for rec in print_records:
        if f := to_solr_multi(rec, "852", "q"):
            mat_held: set[str] = set(f)
            material_held.update(mat_held)
    return list(material_held) or None
