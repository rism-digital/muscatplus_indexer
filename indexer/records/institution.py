import logging
from typing import TypedDict, Optional, List, Dict

import pymarc
import yaml

from indexer.helpers.identifiers import country_code_from_siglum
from indexer.helpers.marc import create_marc
from indexer.helpers.profiles import process_marc_profile
from indexer.helpers.utilities import to_solr_single, to_solr_single_required
from indexer.processors import institution as institution_processor

log = logging.getLogger("muscat_indexer")
institution_profile: Dict = yaml.full_load(open('profiles/sources.yml', 'r'))


class InstitutionIndexDocument(TypedDict):
    id: str
    type: str
    institution_id: str
    name_s: str
    city_s: Optional[str]
    siglum_s: Optional[str]
    country_code_s: Optional[str]
    alternate_names_sm: Optional[List[str]]
    institution_types_sm: Optional[List[str]]
    website_s: Optional[str]
    external_ids: Optional[List[str]]
    related_people_json: Optional[str]
    related_places_json: Optional[str]
    related_institutions_json: Optional[str]
    location_loc: Optional[str]


def create_institution_index_document(institution: str) -> InstitutionIndexDocument:
    record: pymarc.Record = create_marc(institution)
    institution_id: str = f"institution_{to_solr_single_required(record, '001')}"

    institution_core: Dict = {
        "id": institution_id,
        "type": "institution",
        "institution_id": institution_id,
    }

    additional_fields: Dict = process_marc_profile(institution_profile, institution_id, record, institution_processor)
    institution_core.update(additional_fields)

    return institution_core
