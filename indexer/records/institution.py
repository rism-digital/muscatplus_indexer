import logging
from typing import TypedDict, Optional

import pymarc
import ujson
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

    related_institutions = record.get("related_institutions")
    inst_lookup: dict = {}
    if related_institutions:
        all_related_institutions: list = related_institutions.split("\n")
        for inst in all_related_institutions:
            components: list = inst.split("|")
            if len(components) == 2:
                inst_lookup[components[0]] = {"name": components[1]}
            elif len(components) == 3:
                inst_lookup[components[0]] = {"name": components[2], "siglum": components[1]}
            else:
                continue

    institution_core: dict = {
        "id": institution_id,
        "type": "institution",
        "institution_id": institution_id,
        "rism_id": rism_id,
        "has_siglum_b": True if record.get("siglum") else False,
        "source_count_i": record['source_count'],
        "holdings_count_i": record['holdings_count'],
        "now_in_json": ujson.dumps(_get_now_in_json(marc_record, inst_lookup)),
        "created": record["created"].strftime("%Y-%m-%dT%H:%M:%SZ"),
        "updated": record["updated"].strftime("%Y-%m-%dT%H:%M:%SZ")
    }

    additional_fields: dict = process_marc_profile(institution_profile, institution_id, marc_record, institution_processor)
    institution_core.update(additional_fields)

    return institution_core


def _get_now_in_json(record: pymarc.Record, related_institutions: dict) -> Optional[list[dict]]:
    now_in_fields: list[pymarc.Field] = record.get_fields("580")
    if not now_in_fields:
        return None

    all_entries: list = []

    for entry in now_in_fields:
        institution_id = entry["0"]
        if not institution_id:
            log.warning(f"Got a 'now in' record with no identifier.")
            continue

        if institution_id not in related_institutions:
            log.warning("Could not find a 'now in' institution for %s", institution_id)
            continue

        institution_info: dict = related_institutions.get(institution_id)
        now_in: dict = {
            "institution_id": f"institution_{institution_id}",
            "name": f"{institution_info.get('name')}",
            "relationship": "now-in"
        }

        if "siglum" in institution_info:
            now_in["siglum"] = institution_info.get("siglum")

        all_entries.append(now_in)

    return all_entries


