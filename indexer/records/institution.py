from typing import TypedDict, Optional, List, Tuple

import pymarc
import ujson

from indexer.helpers.identifiers import country_code_from_siglum
from indexer.helpers.marc import create_marc
import logging

from indexer.helpers.utilities import to_solr_single, to_solr_single_required, to_solr_multi, get_related_places, \
    get_related_people, get_related_institutions

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

    d: InstitutionIndexDocument = {
        "id": institution_id,
        "type": "institution",
        "institution_id": institution_id,
        "name_s": to_solr_single_required(record, '110', 'a'),
        "city_s": to_solr_single(record, '110', 'c'),
        "siglum_s": to_solr_single(record, '110', 'g'),
        "country_code_s": _get_country_code(record),
        "alternate_names_sm": to_solr_multi(record, '410', 'a'),
        "institution_types_sm": to_solr_multi(record, "368", "a"),
        "website_s": to_solr_single(record, "371", "u"),
        "external_ids": _get_external_ids(record),
        "related_people_json": ujson.dumps(p) if (p := get_related_people(record)) else None,
        "related_places_json": ujson.dumps(p) if (p := get_related_places(record)) else None,
        "related_institutions_json": ujson.dumps(p) if (p := get_related_institutions(record)) else None,
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
