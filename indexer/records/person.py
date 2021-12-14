import logging
from typing import Dict, List, TypedDict, Optional

import pymarc
import yaml

from indexer.helpers.marc import create_marc
from indexer.helpers.profiles import process_marc_profile
from indexer.helpers.utilities import (
    to_solr_single_required
)
from indexer.processors import person as person_processor

log = logging.getLogger("muscat_indexer")
person_profile: Dict = yaml.full_load(open('profiles/people.yml', 'r'))


class PersonIndexDocument(TypedDict):
    id: str
    type: str
    person_id: str
    name_s: Optional[str]
    date_statement_s: Optional[str]
    other_dates_s: Optional[str]
    variant_names_sm: Optional[List]
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


def create_person_index_document(record: Dict) -> Dict:
    marc_record: pymarc.Record = create_marc(record['marc_source'])
    rism_id: str = to_solr_single_required(marc_record, '001')
    person_id: str = f"person_{rism_id}"

    # For the source count we take the literal count *except* for the Anonymous user,
    # since that throws everything off.
    core_person: Dict = {
        "type": "person",
        "id": person_id,
        "person_id": person_id,
        "rism_id": rism_id,
        "source_count_i": record['source_count'] if rism_id != "30004985" else 0,
        "holdings_count_i": record['holdings_count'],
        "created": record["created"].strftime("%Y-%m-%dT%H:%M:%SZ"),
        "updated": record["updated"].strftime("%Y-%m-%dT%H:%M:%SZ")
    }

    additional_fields: Dict = process_marc_profile(person_profile, person_id, marc_record, person_processor)
    core_person.update(additional_fields)

    return core_person
