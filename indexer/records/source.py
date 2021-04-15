import itertools
import logging
from collections import defaultdict
from datetime import datetime
from typing import TypedDict, Optional, List, Dict, Tuple

import pymarc
import ujson

from indexer.helpers.datelib import parse_date_statement
from indexer.helpers.identifiers import RECORD_TYPES_BY_ID, country_code_from_siglum
from indexer.helpers.marc import create_marc
from indexer.helpers.utilities import (
    to_solr_single_required,
    to_solr_single,
    to_solr_multi,
    normalize_id,
    external_resource_json,
    ExternalResourceDocument,
    get_related_people,
    get_related_institutions
)
from indexer.records.holding import HoldingIndexDocument, holding_index_document

log = logging.getLogger("muscat_indexer")


# Forward-declare some typed dictionaries. These both help to ensure the documents getting indexed
# contain the expected fields of the expected types, and serve as a point of reference to know
# what fields are on what type of record in Solr.
MaterialGroupFields = Dict[str, List]


class MaterialGroupIndexDocument(TypedDict, total=False):
    id: str
    type: str
    source_id: str
    group_num: str
    parts_held: Optional[List[str]]
    extent: Optional[List[str]]
    source_type: Optional[List[str]]
    plate_numbers: Optional[List[str]]


class IncipitIndexDocument(TypedDict):
    id: str
    type: str
    source_id: str
    incipit_num_i: int
    work_num_s: str
    music_incipit_s: Optional[str]
    text_incipit_s: Optional[str]
    role_s: Optional[str]
    title_s: Optional[str]
    key_mode_s: Optional[str]
    key_s: Optional[str]
    timesig_s: Optional[str]
    clef_s: Optional[str]
    general_notes_sm: Optional[List[str]]
    scoring_sm: Optional[List[str]]


class SourceSubjectIndexDocument(TypedDict):
    id: str
    type: str
    source_id: str
    term_s: str


class SourceIndexDocument(TypedDict):
    id: str
    type: str
    source_id: str
    source_membership_id: str
    source_membership_title_s: Optional[str]
    source_membership_json: Optional[str]
    is_item_record_b: bool
    subtype_s: str
    main_title_s: str
    source_title_s: Optional[str]
    standardized_title_s: Optional[str]
    key_mode_s: Optional[str]
    scoring_summary_sm: Optional[List[str]]
    additional_title_s: Optional[str]
    variant_title_s: Optional[str]
    creator_name_s: Optional[str]
    creator_id: Optional[str]
    source_members_sm: Optional[List[str]]
    related_people_sm: Optional[List[str]]
    related_people_ids: Optional[List[str]]
    institutions_sm: Optional[List[str]]
    institutions_ids: Optional[List[str]]
    opus_numbers_sm: Optional[List[str]]
    general_notes_sm: Optional[List[str]]
    binding_notes_sm: Optional[List[str]]
    contents_notes_sm: Optional[List[str]]
    description_summary_sm: Optional[List[str]]
    source_type_sm: Optional[List[str]]
    instrumentation_sm: Optional[List[str]]
    subjects_sm: Optional[List[str]]
    num_holdings_i: Optional[int]
    holding_institutions_sm: Optional[List[str]]
    holding_institutions_ids: Optional[List[str]]
    date_statements_sm: Optional[List[str]]
    date_ranges_im: Optional[List[int]]
    country_code_s: Optional[str]
    siglum_s: Optional[str]
    shelfmark_s: Optional[str]
    former_shelfmarks_sm: Optional[List[str]]
    liturgical_festivals_sm: Optional[List[str]]
    language_text_sm: Optional[List[str]]
    language_libretto_sm: Optional[List[str]]
    language_original_sm: Optional[List[str]]
    has_digitization_b: bool
    has_iiif_manifest_b: bool
    material_groups_json: Optional[str]
    subjects_json: Optional[str]
    rism_series_json: Optional[str]
    related_people_json: Optional[str]
    related_institutions_json: Optional[str]
    creator_json: Optional[str]
    external_resources_json: Optional[str]
    liturgical_festivals_json: Optional[str]
    instrumentation_json: Optional[str]
    created: datetime
    updated: datetime


def create_source_index_documents(record: Dict) -> List:
    source: str = record['marc_source']
    # A source is always either its own member, or belonging to a membership
    # of a "parent" source.
    membership_id: int = m if (m := record.get('source_id')) else record['id']
    record_type_id: int = record['record_type']

    record_subtype: str = RECORD_TYPES_BY_ID.get(record_type_id)
    marc_record: pymarc.Record = create_marc(source)

    source_id: str = f"source_{normalize_id(to_solr_single_required(marc_record, '001'))}"
    people_marc_ids: List = to_solr_multi(marc_record, "700", "0") or []
    people_ids: List = [f"person_{p}" for p in people_marc_ids]

    creator_id: Optional[str] = f"person_{cid}" if (cid := to_solr_single(marc_record, '100', '0')) else None

    institution_marc_ids: List = to_solr_multi(marc_record, "710", "0") or []
    institution_ids: List = [f"institution_{i}" for i in institution_marc_ids]

    num_holdings: int = record.get("holdings_count")

    main_title: str = _get_main_title(marc_record)
    source_title: str = to_solr_single_required(marc_record, "245", "a")

    parent_record_type_id: Optional[int] = record.get("parent_record_type")
    source_membership_json: Optional[Dict] = None
    if parent_record_type_id:
        source_membership_json = {
            "source_id": f"source_{membership_id}",
            "main_title": record.get("parent_title"),
        }

    manuscript_holdings: List = _get_manuscript_holdings(marc_record, source_id, main_title) or []
    holding_orgs: List = _get_holding_orgs(manuscript_holdings, record.get("holdings_org"), record.get("parent_holdings_org")) or []
    try:
        holding_orgs_ids: List = _get_holding_orgs_ids(manuscript_holdings, record.get("holdings_marc"), record.get("parent_holdings_marc")) or []
    except:
        log.error("Problem parsing holdings for record %s", source_id)

    related_people: Optional[List] = get_related_people(marc_record, source_id, "source", fields=("700",), ungrouped=True)
    related_institutions: Optional[List] = get_related_institutions(marc_record, source_id, "source", fields=("710",))
    creator: Optional[List] = get_related_people(marc_record, source_id, "source", fields=("100",))
    # If we have a creator, inject the 'cre' relationship code into the record, since it's only implied in the 100
    # field, not explicitly encoded.
    if creator:
        creator[0]["relationship"] = "cre"

    created: datetime = record["created"].strftime("%Y-%m-%dT%H:%M:%SZ")
    updated: datetime = record["updated"].strftime("%Y-%m-%dT%H:%M:%SZ")

    d: SourceIndexDocument = {
        "id": source_id,
        "type": "source",
        "source_id": source_id,
        # The id of a 'parent' record; can be thought of as a bunch of sources belonging to the same 'group',
        # hence the use of 'membership' but without the implied hierarchy. ('children' cannot themselves have 'children'
        # so the treey is always just two members deep.)
        "source_membership_id": f"source_{membership_id}",
        "source_membership_title_s": record.get("parent_title"),
        "source_membership_json": ujson.dumps(source_membership_json) if source_membership_json else None,
        # item records have a different id from the 'parent' source; this allows filtering out of 'item' records.
        "is_item_record_b": source_id != f"source_{membership_id}",
        "subtype_s": record_subtype,
        "main_title_s": main_title,  # matches the display title form in the OPAC
        "source_title_s": source_title,
        "standardized_title_s": record.get("std_title"),  # uses the std_title column in the Muscat database
        "key_mode_s": k.strip() if (k := to_solr_single(marc_record, "240", "r")) else k,
        "scoring_summary_sm": _get_scoring_summary(marc_record),
        "additional_title_s": to_solr_single(marc_record, "730", "a"),
        "variant_title_s": to_solr_single(marc_record, "246", "a"),
        "creator_name_s": _get_creator_name(marc_record),
        "creator_id": creator_id,
        "source_members_sm": _get_source_membership(marc_record),
        "related_people_sm": to_solr_multi(marc_record, "700", "a"),
        "related_people_ids": people_ids,
        "institutions_sm": to_solr_multi(marc_record, "710", "a"),
        "institutions_ids": institution_ids,
        "opus_numbers_sm": to_solr_multi(marc_record, "383", "b"),
        "general_notes_sm": to_solr_multi(marc_record, "500", "a", ungrouped=True),
        "binding_notes_sm": to_solr_multi(marc_record, "563", "a", ungrouped=True),
        "contents_notes_sm": to_solr_multi(marc_record, "505", "a", ungrouped=True),
        "description_summary_sm": to_solr_multi(marc_record, "520", "a"),
        "source_type_sm": to_solr_multi(marc_record, "593", "a"),
        "instrumentation_sm": to_solr_multi(marc_record, "594", "b"),
        "subjects_sm": to_solr_multi(marc_record, "650", "a"),
        "num_holdings_i": 1 if num_holdings == 0 else num_holdings,  # every source has at least one exemplar
        "holding_institutions_sm": holding_orgs,
        "holding_institutions_ids": holding_orgs_ids,
        "date_statements_sm": to_solr_multi(marc_record, "260", "c"),
        "date_ranges_im": _get_earliest_latest_dates(marc_record),
        "country_code_s": _get_country_code(marc_record),
        "siglum_s": to_solr_single(marc_record, "852", "a"),
        "shelfmark_s": to_solr_single(marc_record, "852", "c"),
        "former_shelfmarks_sm": to_solr_multi(marc_record, "852", "d"),
        "liturgical_festivals_sm": to_solr_multi(marc_record, "657", "a"),
        "language_text_sm": to_solr_multi(marc_record, "041", "a"),
        "language_libretto_sm": to_solr_multi(marc_record, "041", "e"),
        "language_original_sm": to_solr_multi(marc_record, "041", "h"),
        "has_digitization_b": _get_has_digitization(marc_record),
        "has_iiif_manifest_b": _get_has_iiif_manifest(marc_record),
        "material_groups_json": ujson.dumps(mg) if (mg := _get_material_groups(marc_record, source_id)) else None,
        "rism_series_json": ujson.dumps(rs) if (rs := _get_rism_series_json(marc_record)) else None,
        "subjects_json": ujson.dumps(sb) if (sb := _get_subjects(marc_record)) else None,
        "creator_json": ujson.dumps(creator) if creator else None,
        "related_people_json": ujson.dumps(related_people) if related_people else None,
        "related_institutions_json": ujson.dumps(related_institutions) if related_institutions else None,
        "external_resources_json": ujson.dumps(f) if (f := _get_external_resources(marc_record)) else None,
        "liturgical_festivals_json": ujson.dumps(f) if (f := _get_liturgical_festivals(marc_record)) else None,
        "instrumentation_json": ujson.dumps(inst) if (inst := _get_instrumentation(marc_record)) else None,
        "created": created,
        "updated": updated
    }

    # material_groups: List = _get_material_groups(marc_record, source_id) or []
    incipits: List = _get_incipits(marc_record, source_id) or []

    # Create a list of all the Solr records to send off for indexing, and extend with any additional records if there
    # are results. We don't need to check these, since they're guaranteed to be a list (even if they are empty).
    res: List = [d]

    res.extend(incipits)
    res.extend(manuscript_holdings)

    return res


def _get_main_title(record: pymarc.Record) -> str:
    standardized_title: str = to_solr_single_required(record, '240', 'a')
    arrangement: Optional[str] = to_solr_single(record, '240', 'o')
    excerpts: Optional[str] = to_solr_single(record, '240', 'k')
    key: Optional[str] = to_solr_single(record, '240', 'r')
    score_summary: Optional[str] = to_solr_single(record, '240', 'm')
    siglum: Optional[str] = to_solr_single(record, '852', 'a')
    shelfmark: Optional[str] = to_solr_single(record, '852', 'c')
    opus: Optional[str] = to_solr_single(record, "383", "b")
    source_type: Optional[str] = to_solr_single(record, "593", "a")

    # collect the title statement in a list to be joined later. This is easier than appending strings together!
    title: List[str] = [f"{standardized_title.strip()};"]

    if excerpts:
        title.append(f" ({excerpts});")
    if arrangement:
        title.append(f" ({arrangement});")
    if source_type:
        title.append(f" {source_type};")
    if shelfmark and siglum:
        title.append(f" {siglum} {shelfmark}")

    # Be sure to stop off any trailing semicolons if there are any, and any leading or
    # trailing spaces
    return "".join(title).rstrip(";")


def _get_creator_name(record: pymarc.Record) -> Optional[str]:
    creator: pymarc.Field = record["100"]
    if not creator:
        return None

    name: str = creator["a"]
    dates: str = f" ({d})" if (d := to_solr_single(record, "100", "d")) else ""

    return f"{name}{dates}"


def _get_subjects(record: pymarc.Record) -> Optional[List[Dict]]:
    if '650' not in record:
        return None

    subject_fields: List[pymarc.Field] = record.get_fields("650")
    ret: List = []
    for field in subject_fields:
        d = {
            "id": f"subject_{field['0']}",
            "subject": field["a"]
        }
        # Ensure we remove any None values
        ret.append({k: v for k, v in d.items() if v})

    return ret


def _get_source_membership(record: pymarc.Record) -> Optional[List]:
    members: Optional[List] = record.get_fields("774")
    if not members:
        return None

    ret: List = []

    for tag in members:
        member_id: Optional[str] = tag["w"] or None
        if not member_id:
            continue

        member_type: Optional[str] = tag["4"] or None
        # Create an ID like "holding_12345" or "source_4567" (default)
        ret.append(
            f"{'source' if not member_type else member_type}_{normalize_id(member_id)}"
        )

    return ret


def __mg_plate(field: pymarc.Field) -> MaterialGroupFields:
    res: MaterialGroupFields = {
        "plate_numbers": field.get_subfields('a')
    }

    return res


def __mg_pub(field: pymarc.Field) -> MaterialGroupFields:
    res: MaterialGroupFields = {
        "place_publication": field.get_subfields("a"),
        "name_publisher": field.get_subfields("b"),
        "date_statements": field.get_subfields("c")
    }

    return res


def __mg_phys(field: pymarc.Field) -> MaterialGroupFields:
    res: MaterialGroupFields = {
        "physical_extent": field.get_subfields("a")
    }

    return res


def __mg_special(field: pymarc.Field) -> MaterialGroupFields:
    res: MaterialGroupFields = {
        "printing_techniques": field.get_subfields("d"),
        "book_formats": field.get_subfields("m")
    }

    return res


def __mg_general(field: pymarc.Field) -> MaterialGroupFields:
    res: MaterialGroupFields = {
        "general_notes": field.get_subfields("a")
    }

    return res


def __mg_binding(field: pymarc.Field) -> MaterialGroupFields:
    res: MaterialGroupFields = {
        "binding_notes": field.get_subfields("a")
    }

    return res


def __mg_parts(field: pymarc.Field) -> MaterialGroupFields:
    res: MaterialGroupFields = {
        "parts_held": field.get_subfields('a'),
        "parts_extent": e if (e := field.get_subfields('b')) else []
    }
    return res


def __mg_watermark(field: pymarc.Field) -> MaterialGroupFields:
    res: MaterialGroupFields = {
        "watermark_notes": field.get_subfields("a")
    }
    return res


def __mg_type(field: pymarc.Field) -> MaterialGroupFields:
    res: MaterialGroupFields = {
        "source_type": field.get_subfields('a')
    }

    return res


def __mg_add_name(field: pymarc.Field) -> MaterialGroupFields:
    person: Dict = {}

    if n := field["a"]:
        person["name"] = n

    if i := field['0']:
        person["other_person_id"] = f"person_{normalize_id(i)}"

    if r := field["4"]:
        person["relationship"] = r

    if q := field["j"]:
        person["qualifier"] = q

    res: MaterialGroupFields = {
        "people": [person]
    }

    return res


def __mg_add_inst(field: pymarc.Field) -> MaterialGroupFields:
    institution: Dict = {}

    if n := field['a']:
        institution["name"] = n

    if d := field['b']:
        institution["department"] = d

    if i := field['0']:
        institution["institution_id"] = f"institution_{normalize_id(i)}"

    if q := field['j']:
        institution["qualifier"] = q

    if r := field['4']:
        institution["relationship"] = r

    res: MaterialGroupFields = {
        "institutions": [institution]
    }

    return res


def __mg_external(field: pymarc.Field) -> MaterialGroupFields:
    res: MaterialGroupFields = {
        "external_resources": [external_resource_json(field)]
    }
    return res


def _get_material_groups(record: pymarc.Record, source_id: str) -> Optional[List[MaterialGroupIndexDocument]]:
    """
    To get all the material groups we must first find all the members of each group, and then
    process the individual fields belonging to that group. Every source should always have a group
    01, and some have multiple material groups numbered 02, 03, etc.

    First we define a mapping between the fields and a function that will process that field. Each
    of these functions will return a dictionary containing one or more fields to add to the material
    group, depending on the contents of that field. These fields will be added to the 'main' material
    group record.

    Then we fetch the fields from the records and sort them into a dictionary grouped by their field.

    Then for each group we create the material group dictionary by iterating over the fields and creating
    the MaterialGroupIndexDocument from the contents of the available fields. Each material group is then
    appended to a list that is returned and stored on the source record.

    :param record: A pymarc.Record instance
    :return: A list of MaterialGroupIndexDocument instances
    """
    log.debug("Indexing material groups")

    # Set the mapping between the MARC field and a function to handle processing that field
    # for the material group. Each function takes the field as the only argument, producing
    # a dictionary of one or more fields to be sent to Solr.
    member_fields: Dict = {
        "028": __mg_plate,
        "260": __mg_pub,
        "300": __mg_phys,
        "340": __mg_special,
        "500": __mg_general,
        "563": __mg_binding,
        "590": __mg_parts,
        "592": __mg_watermark,
        "593": __mg_type,
        "700": __mg_add_name,
        "710": __mg_add_inst,
        "856": __mg_external
    }

    # Filter any field instances that do not declare themselves part of a group ($8). This is
    # important especially for the fields that can occur on both the main record and in the
    # material group records, e.g., 700, 710, 856.
    field_instances: List[pymarc.Field] = [f for f in record.get_fields(*member_fields.keys()) if f['8']]

    if not field_instances:
        return None

    # groupby needs the data to be pre-sorted.
    data = sorted(field_instances, key=lambda f: str(f['8']))
    field_groups: List = []

    # Organizes the fields by material groups. Creates a tuple of (groupnum, [fields...]) and appends
    # it to the field groups list.
    for k, g in itertools.groupby(data, key=lambda f: str(f['8'])):
        field_groups.append((k, list(g)))

    res: List = []
    for gpnum, fields in field_groups:
        # The field group will be all multivalued fields, and the base group
        # will be single-valued fields. These will be merged later to form
        # the whole group. We use a defaultdict here so that we can just assume
        # that the fields will always be a list, and we don't have to check whether
        # it exists before adding new values to it.
        field_group = defaultdict(list)
        base_group: MaterialGroupIndexDocument = {
            "id": f"mg_{gpnum}",
            "group_num": f"{gpnum}",
            "source_id": source_id
        }

        for field in fields:
            field_res: MaterialGroupFields = member_fields[field.tag](field)
            if not field_res:
                continue

            for subf, subv in field_res.items():
                # Filter out any empty values
                if not subv:
                    continue
                field_group[subf].extend(subv)

        # Cast the sets to a list so that they can be serialized by the JSON encoder.
        # values_list: Dict = {k: list(v) for k, v in field_group.items()}

        # Join the field group to the base group, and then append the combined dict
        # to the list of results to be indexed.
        base_group.update(field_group)
        res.append(base_group)

    return res


def __incipit(field: pymarc.Field, source_id: str, num: int) -> IncipitIndexDocument:
    work_number: str = f"{field['a']}.{field['b']}.{field['c']}"

    return {
        "id": f"{source_id}_incipit_{num}",
        "type": "incipit",
        "source_id": source_id,
        "incipit_num_i": num,
        "music_incipit_s": field['p'],
        "text_incipit_s": field['t'],
        "title_s": field['d'],
        "role_s": field['e'],
        "work_num_s": work_number,
        "key_mode_s": field['r'],
        "key_s": field['n'],
        "timesig_s": field['o'],
        "clef_s": field['g'],
        "general_notes_sm": field.get_subfields('q'),
        "scoring_sm": field.get_subfields('z')
    }


def _get_incipits(record: pymarc.Record, source_id: str) -> Optional[List]:
    incipits: List = record.get_fields("031")
    if not incipits:
        return None

    return [__incipit(f, source_id, num) for num, f in enumerate(incipits)]


def _get_country_code(record: pymarc.Record) -> Optional[str]:
    siglum: Optional[str] = to_solr_single(record, "852", "a")
    if not siglum:
        return None

    return country_code_from_siglum(siglum)


def _get_rism_series_json(record: pymarc.Record) -> Optional[List[Dict]]:
    fields: List[pymarc.Field] = record.get_fields("596")
    if not fields:
        return None

    ret: List = []
    for field in fields:
        d = {
            "reference": field['a'],
            "series_id": field['b']
        }
        ret.append({k: v for k, v in d.items() if v})

    return ret


def _get_manuscript_holdings(record: pymarc.Record, source_id: str, main_title: str) -> Optional[List[HoldingIndexDocument]]:
    """
        Create a holding record for sources that do not actually have a holding record, e.g., manuscripts
        This is so that we can provide a unified interface for searching all holdings of an institution
        using the holding record mechanism, rather than a mixture of several different record types.
    """
    # First check to see if the record has 852 fields; if it doesn't, skip trying to process any further.
    if "852" not in record:
        return None

    source_num: str = to_solr_single_required(record, '001')
    holding_institution_ident: Optional[str] = to_solr_single(record, "852", "x")
    # Since these are for MSS, the holding ID is created by tying together the source id and the institution id; this
    # should result in a unique identifier for this holding record.
    holding_id: str = f"holding_{holding_institution_ident}-{source_id}"
    holding_record_id: str = f"{holding_institution_ident}-{source_num}"

    return [holding_index_document(record, holding_id, holding_record_id, source_id, main_title)]


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
        try:
            m: pymarc.Record = create_marc(rec)
        except AttributeError as e:
            log.error("Could not process MARC record %s", rec)
            raise

        if inst := to_solr_single(m, "852", "x"):
            ids.add(f"institution_{inst}")

    return list(ids)


def _get_external_resources(record: pymarc.Record) -> Optional[List[ExternalResourceDocument]]:
    """
    Fetch the external links defined on the record. Note that this will *not* index the links that are linked to
    material group descriptions -- those are handled in the material group indexing section above.
    :param record: A pymarc record
    :return: A list of external links. This will be serialized to a string for storage in Solr.
    """
    ungrouped_ext_links: List[ExternalResourceDocument] = [external_resource_json(f) for f in record.get_fields("856") if f and '8' not in f]
    if not ungrouped_ext_links:
        return None

    return ungrouped_ext_links


def _get_has_digitization(record: pymarc.Record) -> bool:
    digitization_links: List = [f for f in record.get_fields("856") if 'x' in f and f['x'] == "Digitalization"]

    return len(digitization_links) > 0


def _get_has_iiif_manifest(record: pymarc.Record) -> bool:
    iiif_manifests: List = [f for f in record.get_fields("856") if 'x' in f and f['x'] == "IIIF"]

    return len(iiif_manifests) > 0


def __liturgical_festival(field: pymarc.Field) -> Dict:
    d = {
        "id": f"festival_{field['0']}",
        "name": f"{field['a']}"
    }
    return {k: v for k, v in d.items() if v}


def _get_liturgical_festivals(record: pymarc.Record) -> Optional[List[Dict]]:
    fields: List = record.get_fields("657")
    if not fields:
        return None

    return [__liturgical_festival(f) for f in fields]


def __instrumentation(field: pymarc.Field) -> Dict:
    d = {
        "voice_instrument": field["b"],
        "number": field["c"]
    }

    return {k: v for k, v in d.items() if v}


def _get_instrumentation(record: pymarc.Record) -> Optional[List[Dict]]:
    fields: List = record.get_fields("594")
    if not fields:
        return None

    return [__instrumentation(i) for i in fields]


def _get_earliest_latest_dates(record: pymarc.Record) -> Optional[List[int]]:
    earliest_dates: List[int] = []
    latest_dates: List[int] = []
    date_statements: Optional[List] = to_solr_multi(record, "260", "c")

    # if no date statement, return an empty dictionary. This allows us to keep a consistent return type
    # since a call to `.update()` with an empty dictionary won't do anything.
    if not date_statements:
        return None

    for statement in date_statements:
        try:
            earliest, latest = parse_date_statement(statement)
        except Exception as e:  # noqa
            # The breadth of errors mean we could spend all day catching things, so in this case we use
            # a blanket exception catch and then log the statement to be fixed so that we might fix it later.
            log.error("Error parsing date statement %s: %s", statement, e)
            raise

        if earliest:
            earliest_dates.append(earliest)

        if latest:
            latest_dates.append(latest)

    earliest_date: int = min(earliest_dates) if earliest_dates else -9999
    latest_date: int = max(latest_dates) if latest_dates else 9999

    # If neither date was parseable, don't pretend we have a date.
    if earliest_date == -9999 and latest_date == 9999:
        return None

    return [earliest_date, latest_date]


def _get_scoring_summary(record: pymarc.Record) -> Optional[List]:
    """Takes a list of instrument fields and ensures that they are split into a multi-valued representation. So a
       value of:
       ["V, orch", "B, guit"]

       Would result in an instrument list of:

       ["V", "orch", "B", "guit"]
       """
    fields: Optional[List] = to_solr_multi(record, "240", "m")
    if not fields:
        return None

    all_instruments: List = list({val.strip() for field in fields for val in field.split(",") if val and val.strip()})
    return all_instruments


