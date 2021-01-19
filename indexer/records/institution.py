from typing import TypedDict, Optional, List, Tuple

import pymarc

from indexer.helpers.identifiers import country_code_from_siglum
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
    country_code_s: Optional[str]
    alternate_names_sm: Optional[List[str]]
    location_loc: Optional[str]
    related_institutions_sm: Optional[List[str]]
    website_s: Optional[str]

def create_institution_index_document(institution: str) -> InstitutionIndexDocument:
    record: pymarc.Record = create_marc(institution)
    institution_id: str = f"institution_{to_solr_single_required(record, '001')}"
    related_institutions_ids: List = to_solr_multi(record, "710", "0") or []

    d: InstitutionIndexDocument = {
        "id": institution_id,
        "type": "institution",
        "institution_id": institution_id,
        "name_s": to_solr_single_required(record, '110', 'a'),
        "city_s": to_solr_single(record, '110', 'c'),
        "siglum_s": to_solr_single(record, '110', 'g'),
        "country_code_s": _get_country_code(record),
        "alternate_names_sm": to_solr_multi(record, '410', 'a'),
        "website_s": to_solr_single(record, "371", "u"),
        "external_ids": _get_external_ids(record),
        "related_institutions_sm": [f"institution_{i}" for i in related_institutions_ids if i],
        "location_loc": _get_location(record)
    }

    return d


def _get_location(record: pymarc.Record) -> Optional[str]:
    if record['034'] and (lon := record['034']['d']) and (lat := record['034']['f']):
        return f"{lat},{lon}"

    return None


def _get_country_code(record: pymarc.Record) -> Optional[str]:
    siglum: Optional[str] = to_solr_single(record, "110", "g")
    if not siglum:
        return None

    return country_code_from_siglum(siglum)


def _get_external_ids(record: pymarc.Record) -> Optional[List]:
    """Converts DNB and VIAF Ids to a namespaced identifier suitable for expansion later. """
    ids: List = record.get_fields('024')
    if not ids:
        return None

    return [f"{idf['2'].lower()}:{idf['a']}" for idf in ids if (idf and idf['2'])]
