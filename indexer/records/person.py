import logging
import uuid
from typing import Dict, List, TypedDict, Optional

import pymarc

from indexer.helpers.marc import create_marc, record_value_lookup, id_field_lookup
from indexer.helpers.utilities import to_solr_single, to_solr_single_required, to_solr_multi

log = logging.getLogger("muscat_indexer")


class PersonIndexDocument(TypedDict):
    id: str
    type: str
    person_id: str
    name_s: Optional[str]
    date_statement_s: Optional[str]
    name_variants_sm: Optional[List]
    gender_s: Optional[str]
    roles_sm: Optional[List]
    external_ids: Optional[List]
    boost: int


def create_person_index_documents(record: Dict) -> List:
    marc_record: pymarc.Record = create_marc(record['marc_source'])
    pid: str = f"person_{to_solr_single_required(marc_record, '001')}"

    d: PersonIndexDocument = {
        "type": "person",
        "id": pid,
        "person_id": pid,
        "name_s": to_solr_single(marc_record, '100', 'a'),
        "date_statement_s": to_solr_single(marc_record, '100', 'd'),
        "name_variants_sm": to_solr_multi(marc_record, '400', 'a'),
        "gender_s": to_solr_single(marc_record, '375', 'a'),
        "roles_sm": to_solr_multi(marc_record, '550', 'a'),
        "external_ids": _get_external_ids(marc_record),
        "boost": record.get("source_count", 0)
    }

    related_people: List = _get_related_people(marc_record) or []

    return [d, *related_people]


def _get_external_ids(record: pymarc.Record) -> Optional[List]:
    """Converts DNB and VIAF Ids to a namespaced identifier suitable for expansion later. """
    ids: List = record.get_fields('024')
    if not ids:
        return None

    return [f"{idf['2'].lower()}:{idf['a']}" for idf in ids if (idf and idf['2'])]


def __related_person(field: pymarc.Field, related_id: str) -> Dict:
    """
    Generate a related person record. The target of the relationship is given in the person_id field,
    while the source of the relationship is given in the related_id field.

    :param field: The pymarc field for the relationship
    :param related_id: The ID of the source person for the relationship
    :return: A Solr record for the person relationship
    """
    return {
        "id": f"{uuid.uuid4()}",
        "type": "related_person",
        "name_s": field['a'],
        "relationship_s": field['i'],
        "person_id": f"person_{field['0']}",
        "related_id": related_id
    }


def _get_related_people(record: pymarc.Record) -> Optional[List[Dict]]:
    people: List = record.get_fields('500')
    if not people:
        return None

    related_id: str = f"person_{to_solr_single_required(record, '001')}"

    return [__related_person(p, related_id) for p in people if p]
