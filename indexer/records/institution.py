import logging
from typing import TypedDict, Optional

import pymarc
import yaml

from indexer.helpers.marc import create_marc
from indexer.helpers.profiles import process_marc_profile
from indexer.helpers.utilities import to_solr_single_required
from indexer.processors import institution as institution_processor

log = logging.getLogger("muscat_indexer")
institution_profile: dict = yaml.full_load(open('profiles/institutions.yml', 'r'))


class InstitutionIndexDocument(TypedDict):
    id: str
    type: str
    institution_id: str
    name_s: str
    city_s: Optional[str]
    siglum_s: Optional[str]
    country_code_s: Optional[str]
    alternate_names_sm: Optional[list[str]]
    institution_types_sm: Optional[list[str]]
    website_s: Optional[str]
    external_ids: Optional[list[str]]
    related_people_json: Optional[str]
    related_places_json: Optional[str]
    related_institutions_json: Optional[str]
    location_loc: Optional[str]


def create_institution_index_document(record: dict, cfg: dict) -> InstitutionIndexDocument:
    marc_record: pymarc.Record = create_marc(record['marc_source'])
    rism_id: str = to_solr_single_required(marc_record, '001')
    institution_id: str = f"institution_{rism_id}"

    institution_core: dict = {
        "id": institution_id,
        "type": "institution",
        "institution_id": institution_id,
        "rism_id": rism_id,
        "source_count_i": record['source_count'],
        "holdings_count_i": record['holdings_count'],
        "created": record["created"].strftime("%Y-%m-%dT%H:%M:%SZ"),
        "updated": record["updated"].strftime("%Y-%m-%dT%H:%M:%SZ")
    }

    additional_fields: dict = process_marc_profile(institution_profile, institution_id, marc_record, institution_processor)
    institution_core.update(additional_fields)

    return institution_core
