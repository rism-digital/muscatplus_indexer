from typing import TypedDict, Optional, List, Tuple

import pymarc

from indexer.helpers.marc import create_marc
import logging

from indexer.helpers.utilities import to_solr_single, to_solr_single_required, to_solr_multi

log = logging.getLogger("muscat_indexer")


class InstitutionIndexDocument(TypedDict):
    id: str
    type: str
    institution_id: str
    name_s: str
    city_s: Optional[str]
    siglum_s: Optional[str]
    alternate_names_sm: Optional[List[str]]
    location_loc: Optional[str]


def create_institution_index_document(institution: str) -> InstitutionIndexDocument:
    record: pymarc.Record = create_marc(institution)

    d: InstitutionIndexDocument = {
        "id": f"institution_{to_solr_single_required(record, '001')}",
        "type": "institution",
        "institution_id": to_solr_single_required(record, '001'),
        "name_s": to_solr_single_required(record, '110', 'a'),
        "city_s": to_solr_single(record, '110', 'c'),
        "siglum_s": to_solr_single(record, '110', 'g'),
        "alternate_names_sm": to_solr_multi(record, '410', 'a'),
        "location_loc": _get_location(record)
    }

    return d


def _get_location(record: pymarc.Record) -> Optional[str]:
    if record['034'] and (lon := record['034']['d']) and (lat := record['034']['f']):
        return f"{lat},{lon}"

    return None
