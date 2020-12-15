import logging
from typing import TypedDict, Optional, List

import pymarc

from indexer.helpers.identifiers import country_code_from_siglum
from indexer.helpers.marc import create_marc
from indexer.helpers.utilities import to_solr_single, to_solr_multi

log = logging.getLogger("muscat_indexer")


class HoldingIndexDocument(TypedDict):
    id: str
    type: str
    source_membership_id: str
    siglum_s: Optional[str]
    country_code_s: Optional[str]
    holding_institution_s: Optional[str]
    holding_institution_id: Optional[str]
    shelfmark_s: Optional[str]
    material_held_sm: Optional[List[str]]


def create_holding_index_document(source: str, record_id: int, membership_id: int) -> HoldingIndexDocument:
    record: pymarc.Record = create_marc(source)

    d: HoldingIndexDocument = {
        "id": f"holding_{record_id}",
        "type": "holding",
        "source_membership_id": f"source_{membership_id}",
        "siglum_s": to_solr_single(record, "852", "a"),
        "country_code_s": _get_country_code(record),
        "holding_institution_s": to_solr_single(record, '852', 'e'),
        "holding_institution_id": f"institution_{to_solr_single(record, '852', 'x')}",
        "shelfmark_s": to_solr_single(record, '852', 'c'),
        "material_held_sm": to_solr_multi(record, '852', 'q')
    }

    return d


def _get_country_code(marc_record: pymarc.Record) -> Optional[str]:
    siglum: Optional[str] = to_solr_single(marc_record, "852", "a")
    if not siglum:
        return None

    return country_code_from_siglum(siglum)
