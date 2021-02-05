import logging
from typing import TypedDict, Optional, List, Dict

import pymarc

from indexer.helpers.identifiers import country_code_from_siglum
from indexer.helpers.marc import create_marc
from indexer.helpers.utilities import to_solr_single, to_solr_multi

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


def create_holding_index_document(record: Dict) -> HoldingIndexDocument:
    record_id: int = record['id']
    membership_id: int = record['source_id']

    marc_record: pymarc.Record = create_marc(record['marc_source'])

    d: HoldingIndexDocument = {
        "id": f"holding_{record_id}",
        "type": "holding",
        "holding_id_sni": f"{record_id}",  # Convenience for URL construction; should not be used for lookups.
        "source_id": f"source_{membership_id}",
        "siglum_s": to_solr_single(marc_record, "852", "a"),
        "main_title_s": record["source_title"],
        "country_code_s": _get_country_code(marc_record),
        "institution_s": to_solr_single(marc_record, '852', 'e'),
        "institution_id": f"institution_{to_solr_single(marc_record, '852', 'x')}",
        "shelfmark_s": to_solr_single(marc_record, '852', 'c'),
        "former_shelfmarks_sm": to_solr_multi(marc_record, '852', 'd'),
        "material_held_sm": to_solr_multi(marc_record, '852', 'q')
        # TODO: support 856 for digitized holdings.
    }

    return d


def _get_country_code(marc_record: pymarc.Record) -> Optional[str]:
    siglum: Optional[str] = to_solr_single(marc_record, "852", "a")
    if not siglum:
        return None

    return country_code_from_siglum(siglum)
