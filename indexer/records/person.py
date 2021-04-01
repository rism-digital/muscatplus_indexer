import logging
import operator
from collections import defaultdict
from typing import Dict, List, TypedDict, Optional

import pymarc
import ujson

from indexer.helpers.marc import create_marc
from indexer.helpers.utilities import (
    to_solr_single,
    to_solr_single_required,
    to_solr_multi,
    external_resource_json,
    get_related_places,
    get_related_people,
    get_related_institutions
)

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
    person_id: str = f"person_{to_solr_single_required(marc_record, '001')}"

    d: PersonIndexDocument = {
        "type": "person",
        "id": person_id,
        "person_id": person_id,
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
        "related_people_json": ujson.dumps(p) if (p := get_related_people(marc_record, person_id, "person")) else None,
        "related_places_json": ujson.dumps(p) if (p := get_related_places(marc_record, person_id, "person")) else None,
        "related_institutions_json": ujson.dumps(p) if (p := get_related_institutions(marc_record, person_id, "person")) else None,
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
