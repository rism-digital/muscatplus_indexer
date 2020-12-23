import logging
import itertools
from datetime import datetime
from collections import defaultdict
from typing import TypedDict, Optional, List, Dict

import pymarc

from indexer.helpers.identifiers import RECORD_TYPES_BY_ID, country_code_from_siglum
from indexer.helpers.marc import create_marc
from indexer.helpers.utilities import to_solr_single_required, to_solr_single, to_solr_multi, normalize_id

log = logging.getLogger("muscat_indexer")


# Forward-declare some typed dictionaries. These both help to ensure the documents getting indexed
# contain the expected fields of the expected types, and serve as a point of reference to know
# what fields are on what type of record in Solr.
class PersonRelationshipIndexDocument(TypedDict):
    id: str
    type: str
    source_id: str
    main_title_s: str
    relationship_id: str
    name_s: Optional[str]
    date_statement_s: Optional[str]
    person_id: Optional[str]
    relationship_s: Optional[str]
    qualifier_s: Optional[str]


class InstitutionRelationshipIndexDocument(TypedDict):
    id: str
    type: str
    source_id: str
    relationship_id: str
    main_title_s: str
    name_s: Optional[str]
    institution_id: Optional[str]
    relationship_s: Optional[str]
    qualifier_s: Optional[str]


MaterialGroupFields = Dict[str, List]


class MaterialGroupIndexDocument(TypedDict, total=False):
    id: str
    type: str
    source_id: str
    group_num_s: str
    parts_held_sm: Optional[List[str]]
    extent_sm: Optional[List[str]]
    source_type_sm: Optional[List[str]]
    plate_numbers_sm: Optional[List[str]]


class IncipitIndexDocument(TypedDict):
    id: str
    type: str
    source_id: str
    incipit_num_s: str
    work_num_s: str
    music_incipit_s: Optional[str]
    text_incipit_s: Optional[str]
    title_s: Optional[str]


class SourceSubjectIndexDocument(TypedDict):
    id: str
    type: str
    source_id: str
    term_s: str


class SourceIndexDocument(TypedDict):
    id: str
    type: str
    source_id: str
    source_membership_id: Optional[str]
    source_membership_title_s: Optional[str]
    subtype_s: str
    main_title_s: str
    standardized_title_s: Optional[str]
    source_title_s: Optional[str]
    additional_title_s: Optional[str]
    key_mode_s: Optional[str]
    scoring_summary_sm: Optional[List[str]]
    creator_name_s: Optional[str]
    creator_id: Optional[str]
    opus_numbers_sm: Optional[List[str]]
    general_notes_sm: Optional[List[str]]
    binding_notes_sm: Optional[List[str]]
    description_summary_sm: Optional[List[str]]
    source_type_sm: Optional[List[str]]
    source_members_sm: Optional[List[str]]
    related_people_sm: Optional[List[str]]
    related_people_ids: Optional[List[str]]
    institutions_sm: Optional[List[str]]
    institutions_ids: Optional[List[str]]
    subject_ids: Optional[List[str]]
    num_holdings_i: Optional[int]
    date_statements_sm: Optional[List[str]]
    country_code_s: Optional[str]
    siglum_s: Optional[str]
    shelfmark_s: Optional[str]
    former_shelfmarks_sm: Optional[List[str]]
    holding_institution_id: Optional[str]
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

    subject_marc_ids: List = to_solr_multi(marc_record, "650", "0") or []
    subject_ids: List = [f"subject_{i}" for i in subject_marc_ids]

    holding_institution_ident: Optional[str] = to_solr_single(marc_record, "852", "x")
    holding_institution_id: Optional[str] = f"institution_{holding_institution_ident}" if holding_institution_ident else None

    num_holdings: int = record.get("holdings_count")

    main_title: str = _get_main_title(marc_record)
    source_title: str = to_solr_single_required(marc_record, "245", "a")

    created: datetime = record["created"].strftime("%Y-%m-%dT%H:%M:%SZ")
    updated: datetime = record["updated"].strftime("%Y-%m-%dT%H:%M:%SZ")

    d: SourceIndexDocument = {
        "id": source_id,
        "type": "source",
        "source_id": source_id,
        "source_membership_id": f"source_{membership_id}",
        "source_membership_title_s": record.get("parent_title"),
        "subtype_s": record_subtype,
        "main_title_s": main_title,  # matches the display title form in the OPAC
        "source_title_s": source_title,
        "standardized_title_s": record.get("std_title"),  # uses the std_title column in the Muscat database
        "key_mode_s": to_solr_single(marc_record, "240", "r"),
        "scoring_summary_sm": to_solr_multi(marc_record, "240", "m"),
        "additional_title_s": to_solr_single(marc_record, "730", "a"),
        "creator_name_s": _get_creator_name(marc_record),
        "creator_id": creator_id,
        "source_members_sm": _get_source_membership(marc_record),
        "related_people_sm": to_solr_multi(marc_record, "700", "a"),
        "related_people_ids": people_ids,
        "institutions_sm": to_solr_multi(marc_record, "710", "a"),
        "institutions_ids": institution_ids,
        "opus_numbers_sm": to_solr_multi(marc_record, "383", "b"),
        "general_notes_sm": to_solr_multi(marc_record, "500", "a"),
        "binding_notes_sm": to_solr_multi(marc_record, "563", "a"),
        "description_summary_sm": to_solr_multi(marc_record, "520", "a"),
        "source_type_sm": to_solr_multi(marc_record, "593", "a"),  # A list of all types associated with all material groups; Individual material groups also get their own Solr doc
        "subject_ids": subject_ids,
        "num_holdings_i": num_holdings,
        "date_statements_sm": to_solr_multi(marc_record, "260", "c"),
        "country_code_s": _get_country_code(marc_record),
        "siglum_s": to_solr_single(marc_record, "852", "a"),
        "shelfmark_s": to_solr_single(marc_record, "852", "c"),
        "former_shelfmarks_sm": to_solr_multi(marc_record, "852", "d"),
        "holding_institution_id": holding_institution_id,
        "created": created,
        "updated": updated
    }

    people_relationships: List = _get_people_relationships(marc_record, source_id, main_title) or []
    institution_relationships: List = _get_institution_relationships(marc_record, source_id, main_title) or []
    creator: List = _get_creator(marc_record, source_id, main_title) or []
    material_groups: List = _get_material_groups(marc_record, source_id) or []
    incipits: List = _get_incipits(marc_record, source_id) or []

    # Create a list of all the Solr records to send off for indexing, and extend with any additional records if there
    # are results. We don't need to check these, since they're guaranteed to be a list (even if they are empty).
    res: List = [d]

    res.extend(creator)
    res.extend(people_relationships)
    res.extend(institution_relationships)
    res.extend(material_groups)
    res.extend(incipits)

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


def _get_creator(record: pymarc.Record, source_id: str, source_title: str) -> Optional[List[PersonRelationshipIndexDocument]]:
    creator: pymarc.Field = record['100']
    if not creator:
        return None

    return [{
        "id": f"{source_id}_relationship_creator",
        "type": "source_person_relationship",
        "source_id": source_id,
        "name_s": to_solr_single(record, "100", "a"),
        "main_title_s": source_title,
        "qualifier_s": to_solr_single(record, "100", "j"),
        "date_statement_s": to_solr_single(record, "100", "d"),
        "person_id": f"person_{c}" if (c := to_solr_single(record, "100", "0")) else None,
        "relationship_s": "cre",
        "relationship_id": "creator"
    }]


def __person_relationship(field: pymarc.Field, source_id: str, source_title: str, num: int) -> PersonRelationshipIndexDocument:
    """
    Constructs a document that defines a person's relationship to this source, with enough
    information to provide links to this person, and to filter on their relationship.

    :param field: a pymarc.Field instance
    :return: a Dictionary representing a PersonRelationshipindexDocument.
    """
    relationship_id: str = f"person-{num}"

    return {
        "id": f"{source_id}_relationship_{relationship_id}",
        "source_id": source_id,
        "type": "source_person_relationship",
        "name_s": field["a"] or None,
        "main_title_s": source_title,
        "qualifier_s": field["j"] or None,
        "date_statement_s": field["d"] or None,
        "person_id": f"person_{field['0']}",
        "relationship_s": field["4"] or None,
        "relationship_id": relationship_id
    }


def _get_people_relationships(record: pymarc.Record, source_id: str, source_title: str) -> Optional[List[PersonRelationshipIndexDocument]]:
    """
    Returns a list of people as a child record on this source.

    :param record: a pymarc.Record instance
    :return:
    """
    people: List[pymarc.Field] = record.get_fields("700")
    if not people:
        return None

    return [__person_relationship(p, source_id, source_title, num) for num, p in enumerate(people, 1)]


def __institution_relationship(field: pymarc.Field, source_id: str, source_title: str, num: int) -> InstitutionRelationshipIndexDocument:
    relationship_id: str = f"institution-{num}"

    return {
        "id": f"{source_id}_relationship_{relationship_id}",
        "type": "source_institution_relationship",
        "source_id": source_id,
        "name_s": field["a"] or None,
        "main_title_s": source_title,
        "qualifier_s": field["g"] or None,
        "institution_id": f"institution_{field['0']}",
        "relationship_s": field["4"] or None,
        "relationship_id": relationship_id
    }


def _get_institution_relationships(record: pymarc.Record, source_id: str, source_title: str) -> Optional[List[InstitutionRelationshipIndexDocument]]:
    institutions: [pymarc.Field] = record.get_fields("710")
    if not institutions:
        return None

    return [__institution_relationship(i, source_id, source_title, num) for num, i in enumerate(institutions, 1)]


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
        "plate_numbers_sm": field.get_subfields('a')
    }

    return res


def __mg_pub(field: pymarc.Field) -> MaterialGroupFields:
    res: MaterialGroupFields = {
        "place_publication_sm": field.get_subfields("a"),
        "name_publisher_sm": field.get_subfields("b"),
        "date_statements_sm": field.get_subfields("c")
    }

    return res


def __mg_phys(field: pymarc.Field) -> MaterialGroupFields:
    res: MaterialGroupFields = {
        "physical_extent_sm": field.get_subfields("a")
    }

    return res


def __mg_special(field: pymarc.Field) -> MaterialGroupFields:
    pass


def __mg_general(field: pymarc.Field) -> MaterialGroupFields:
    res: MaterialGroupFields = {
        "general_notes_sm": field.get_subfields("a")
    }

    return res


def __mg_binding(field: pymarc.Field) -> MaterialGroupFields:
    res: MaterialGroupFields = {
        "binding_notes_sm": field.get_subfields("a")
    }

    return res


def __mg_parts(field: pymarc.Field) -> MaterialGroupFields:
    res: MaterialGroupFields = {
        "parts_held_sm": field.get_subfields('a'),
        "extent_sm": field.get_subfields('b')
    }
    return res


def __mg_watermark(field: pymarc.Field) -> MaterialGroupFields:
    pass


def __mg_type(field: pymarc.Field) -> MaterialGroupFields:
    res: MaterialGroupFields = {
        "source_type_sm": field.get_subfields('a')
    }

    return res


def __mg_add_name(field: pymarc.Field) -> MaterialGroupFields:
    res: MaterialGroupFields = {
        "people_sm": field.get_subfields('a'),
        "people_ids": [f"person_{normalize_id(f)}" for f in field.get_subfields('0') if f]
    }

    return res


def __mg_add_inst(field: pymarc.Field) -> MaterialGroupFields:
    res: MaterialGroupFields = {
        "institutions_sm": field.get_subfields("a"),
        "institutions_ids": [f"institution_{normalize_id(f)}" for f in field.get_subfields("0") if f]
    }

    return res


def __mg_external(field: pymarc.Field) -> MaterialGroupFields:
    pass


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
        # that the fields will always be a set, and we don't have to check whether
        # it exists before adding new values to it. This also has the effect of removing
        # any duplicate values.
        field_group = defaultdict(set)
        base_group: MaterialGroupIndexDocument = {
            "id": f"{source_id}_materialgroup_{gpnum}",
            "source_id": source_id,
            "type": "source_materialgroup",
            "group_num_s": f"{gpnum}"
        }

        for field in fields:
            field_res: MaterialGroupFields = member_fields[field.tag](field)
            if not field_res:
                continue

            for sf, sv in field_res.items():
                field_group[sf].update(sv)

        # Cast the sets to a list so that they can be serialized by the JSON encoder.
        cast_to_list: Dict = {k: list(v) for k, v in field_group.items()}

        # Join the field group to the base group, and then append the combined dict
        # to the list of results to be indexed.
        base_group.update(cast_to_list)
        res.append(base_group)

    return res


def __incipit(field: pymarc.Field, source_id: str, num: int) -> IncipitIndexDocument:
    work_number: str = f"{field['a']}.{field['b']}.{field['c']}"

    return {
        "id": f"{source_id}_incipit_{num}",
        "type": "source_incipit",
        "source_id": source_id,
        "music_incipit_s": field['p'],
        "text_incipit_s": field['t'],
        "title_s": field['d'],
        "incipit_num_s": str(num),
        "work_num_s": work_number
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
