import logging
from typing import TypedDict, Optional, List, Dict

import pymarc
import ujson

from indexer.helpers.identifiers import country_code_from_siglum
from indexer.helpers.marc import create_marc
from indexer.helpers.utilities import to_solr_single, to_solr_multi, external_resource_json

log = logging.getLogger("muscat_indexer")


class HoldingIndexDocument(TypedDict):
    id: str
    type: str
    source_id: str
    main_title_s: str
    holding_id_sni: str  # Convenience for URL construction; should not be used for lookups.
    siglum_s: Optional[str]
    country_code_s: Optional[str]
    institution_s: Optional[str]
    institution_id: Optional[str]
    shelfmark_s: Optional[str]
    former_shelfmarks_sm: Optional[List[str]]
    material_held_sm: Optional[List[str]]
    local_numbers_sm: Optional[List[str]]
    external_resources_json: Optional[str]


def create_holding_index_document(record: Dict) -> HoldingIndexDocument:
    record_id: str = f"{record['id']}"
    membership_id: str = f"{record['source_id']}"
    marc_record: pymarc.Record = create_marc(record['marc_source'])
    holding_id: str = f"holding_{record_id}"
    main_title: str = record["source_title"]

    return holding_index_document(marc_record, holding_id, record_id, membership_id, main_title)


def holding_index_document(marc_record: pymarc.Record, holding_id: str, record_id: str, membership_id: str, main_title: str) -> HoldingIndexDocument:
    d: HoldingIndexDocument = {
        "id": holding_id,
        "type": "holding",
        "source_id": f"{membership_id}",
        "main_title_s": main_title,
        "holding_id_sni": record_id,  # Convenience for URL construction; should not be used for lookups.
        "siglum_s": to_solr_single(marc_record, "852", "a"),
        "country_code_s": _get_country_code(marc_record),
        "institution_s": to_solr_single(marc_record, '852', 'e'),
        "institution_id": f"institution_{to_solr_single(marc_record, '852', 'x')}",
        "shelfmark_s": to_solr_single(marc_record, '852', 'c'),
        "former_shelfmarks_sm": to_solr_multi(marc_record, '852', 'd'),
        "material_held_sm": to_solr_multi(marc_record, '852', 'q'),
        "local_numbers_sm": to_solr_multi(marc_record, "035", "a"),
        "external_resources_json": ujson.dumps(l) if (l := [external_resource_json(f) for f in marc_record.get_fields("856")]) else None
    }

    return d


def _get_country_code(marc_record: pymarc.Record) -> Optional[str]:
    siglum: Optional[str] = to_solr_single(marc_record, "852", "a")
    if not siglum:
        return None

    return country_code_from_siglum(siglum)
