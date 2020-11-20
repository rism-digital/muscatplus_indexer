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
    alternate_names_sm: Optional[List]
    gender_s: Optional[str]
    roles_sm: Optional[List]
    external_ids: Optional[List]
    # related_people: Optional[List[Dict]]


def create_person_index_documents(source: str) -> List:
    record: pymarc.Record = create_marc(source)

    d: PersonIndexDocument = {
        "type": "person",
        "id": f"person_{to_solr_single_required(record, '001')}",
        "person_id": to_solr_single_required(record, '001'),
        "name_s": to_solr_single(record, '100', 'a'),
        "date_statement_s": to_solr_single(record, '100', 'd'),
        "alternate_names_sm": to_solr_multi(record, '400', 'a'),
        "gender_s": to_solr_single(record, '375', 'a'),
        "roles_sm": to_solr_multi(record, '550', 'a'),
        "external_ids": _get_external_ids(record),
        # "related_people": _get_related_people(record),
    }

    related_people: Optional[List] = _get_related_people(record) or []

    return [d, *related_people]


def _get_external_ids(record: pymarc.Record) -> Optional[List]:
    ids: List = record.get_fields('024')
    if not ids:
        return None

    return [f"{idf['2'].lower()}:{idf['a']}" for idf in ids if (idf and idf['2'])]


def __related_person(field: pymarc.Field, related_id: str) -> Dict:
    return {
        "id": f"{uuid.uuid4()}",
        "type": "related_person",
        "name_s": field['a'],
        "relationship_s": field['i'],
        "person_id": field['0'],
        "related_id": related_id
    }


def _get_related_people(record: pymarc.Record) -> Optional[List[Dict]]:
    people: List = record.get_fields('500')
    if not people:
        return None

    related_id: str = f"person_{to_solr_single_required(record, '001')}"

    return [__related_person(p, related_id) for p in people if p]
