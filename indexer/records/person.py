import logging
import operator
from collections import defaultdict
from typing import Dict, List, TypedDict, Optional

import pymarc
import ujson

from indexer.helpers.marc import create_marc
from indexer.helpers.utilities import to_solr_single, to_solr_single_required, to_solr_multi, external_resource_json

log = logging.getLogger("muscat_indexer")


class PersonIndexDocument(TypedDict):
    id: str
    type: str
    person_id: str
    name_s: Optional[str]
    date_statement_s: Optional[str]
    other_dates_s: Optional[str]
    name_variants_sm: Optional[List]
    related_places_sm: Optional[List]
    related_people_sm: Optional[List]
    related_institutions_sm: Optional[List]
    general_notes_sm: Optional[List]
    additional_biography_sm: Optional[List]
    gender_s: Optional[str]
    roles_sm: Optional[List]
    external_ids: Optional[List]
    boost: int
    related_people_json: Optional[str]
    related_places_json: Optional[str]
    related_institutions_json: Optional[str]
    name_variants_json: Optional[str]
    external_resources_json: Optional[str]


def create_person_index_documents(record: Dict) -> List:
    marc_record: pymarc.Record = create_marc(record['marc_source'])
    pid: str = f"person_{to_solr_single_required(marc_record, '001')}"

    d: PersonIndexDocument = {
        "type": "person",
        "id": pid,
        "person_id": pid,
        "name_s": to_solr_single(marc_record, '100', 'a'),
        "date_statement_s": to_solr_single(marc_record, '100', 'd'),
        "other_dates_s": to_solr_single(marc_record, '100', 'y'),
        "name_variants_sm": to_solr_multi(marc_record, '400', 'a'),
        "related_places_sm": to_solr_multi(marc_record, "551", "a"),
        "related_people_sm": to_solr_multi(marc_record, "500", "a"),
        "related_institutions_sm": to_solr_multi(marc_record, "510", "a"),
        "general_notes_sm": to_solr_multi(marc_record, "680", "a"),
        "additional_biography_sm": to_solr_multi(marc_record, "678", "a"),
        "gender_s": to_solr_single(marc_record, '375', 'a'),
        "roles_sm": to_solr_multi(marc_record, '550', 'a'),
        "external_ids": _get_external_ids(marc_record),
        "related_people_json": ujson.dumps(p) if (p := _get_related_people(marc_record)) else None,
        "related_places_json": ujson.dumps(p) if (p := _get_related_places(marc_record)) else None,
        "related_institutions_json": ujson.dumps(p) if (p := _get_related_institutions(marc_record)) else None,
        "name_variants_json": ujson.dumps(n) if (n := _get_name_variants(marc_record)) else None,
        "external_resources_json": ujson.dumps(l) if (l := [external_resource_json(f) for f in marc_record.get_fields("856")]) else None,
        "boost": record.get("source_count", 0)
    }

    return [d]


def _get_external_ids(record: pymarc.Record) -> Optional[List]:
    """Converts DNB and VIAF Ids to a namespaced identifier suitable for expansion later. """
    ids: List = record.get_fields('024')
    if not ids:
        return None

    return [f"{idf['2'].lower()}:{idf['a']}" for idf in ids if (idf and idf['2'])]


def __related_person(field: pymarc.Field, related_id: str, relationship_number: int) -> Dict:
    """
    Generate a related person record. The target of the relationship is given in the person_id field,
    while the source of the relationship is given in the related_id field.

    :param field: The pymarc field for the relationship
    :param related_id: The ID of the source person for the relationship
    :param relationship_number: An integer corresponding to the position of this relationship in the list of all
        relationships for this person. This is because two people can be related in two different ways, so this
        lets us give a unique number to each enumerated relationship.
    :return: A Solr record for the person relationship
    """
    return {
        "id": f"{relationship_number}",
        "name": field['a'],
        "relationship": field['i'],
        "other_person_id": f"person_{field['0']}",
        "this_person_id": related_id
    }


def _get_related_people(record: pymarc.Record) -> Optional[List[Dict]]:
    people: List = record.get_fields('500')
    if not people:
        return None

    related_id: str = f"person_{to_solr_single_required(record, '001')}"

    # NB: enumeration starts at 1
    return sorted([__related_person(p, related_id, i) for i, p in enumerate(people, 1) if p], key=operator.itemgetter("name"))


def __related_place(field: pymarc.Field, person_id: str, relationship_number: int) -> Dict:
    # Note that as of this writing the places are not controlled by the place authorities,
    # so we don't have a place authority ID to store here.

    # TODO: Fix this to point to the place authority once the IDs are stored in MARC. See
    #   https://github.com/rism-digital/muscat/issues/1080

    return {
        "id": f"{relationship_number}",
        "name": field["a"],
        "relationship": field["i"],
        "this_person_id": person_id
    }


def _get_related_places(record: pymarc.Record) -> Optional[List[Dict]]:
    places: List = record.get_fields("551")
    if not places:
        return None

    person_id: str = f"person_{to_solr_single_required(record, '001')}"

    return sorted([__related_place(p, person_id, i) for i, p in enumerate(places, 1) if p], key=operator.itemgetter("name"))


def __related_institution(field: pymarc.Field, person_id: str, relationship_number: int) -> Dict:
    return {
        "id": f"{relationship_number}",
        "name": field["a"],
        "institution_id": f"institution_{field['0']}"
    }


def _get_related_institutions(record: pymarc.Record) -> Optional[List[Dict]]:
    institutions: List = record.get_fields("510")
    if not institutions:
        return None

    person_id: str = f"person_{to_solr_single_required(record, '001')}"

    return sorted([__related_institution(p, person_id, i) for i, p in enumerate(institutions, 1) if p], key=operator.itemgetter("name"))


def _get_name_variants(record: pymarc.Record) -> Optional[List]:
    name_variants = record.get_fields("400")
    if not name_variants:
        return None

    names = defaultdict(list)
    for subf in name_variants:
        if not (n := subf["a"]):
            continue
        # If no $j, then use the "xx" code which will represent "unknown".
        # NB: Some records have "xx" for $j as well, even though it's not an 'official' code.
        category: str = subf["j"] or "xx"
        names[category].append(n)

    # Sort the variants alphabetically and format as list
    name_variants: List = [{"type": k, "variants": sorted(v)} for k, v in names.items()]

    return name_variants
