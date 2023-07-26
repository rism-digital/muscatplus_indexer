import logging
from typing import Optional

from diamm_indexer.helpers.identifiers import transform_rism_id
from indexer.helpers.identifiers import ProjectIdentifiers
from indexer.helpers.solr import exists

log = logging.getLogger("muscat_indexer")


def update_rism_person_document(record, cfg: dict) -> Optional[dict]:
    document_id: Optional[str] = transform_rism_id(record.get('rism_id'))
    if not document_id:
        return None

    if not exists(document_id, cfg):
        log.error("Document %s does not actually exist in RISM!", document_id)
        return None

    d = {
        "id": document_id,
        "has_external_record_b": {"set": True}
    }

    return d


def create_person_index_document(record, cfg: dict) -> dict:
    d = {
        "id": f"diamm_person_{record['id']}",
        "type": "person",
        "project_s": ProjectIdentifiers.DIAMM,
        "record_uri_sni": f"https://www.diamm.ac.uk/people/{record['id']}",
        "name_s": _get_name(record),
        "last_name_s": record.get("last_name"),
        "first_name_s": record.get("first_name"),
        "earliest_year_i": record.get("earliest_year"),
        "latest_year_i": record.get("latest_year"),
        "date_statement_s": _get_date_statement(record)
    }

    return d


def _get_date_statement(record) -> Optional[str]:
    earliest_approx = record.get("earliest_approx")
    latest_approx = record.get("latest_approx")
    earliest = record.get("earliest_year")
    latest = record.get("latest_year")

    earliest_approx_s = f"?" if earliest_approx else ''
    latest_approx_s = f"?" if latest_approx else ''
    earliest_s = f"{earliest}" if earliest and int(earliest) != -1 else ''
    latest_s = f"{latest}" if latest and int(latest) != -1 else ''

    if earliest_s or latest_s:
        return f"{earliest_s}{earliest_approx_s}—{latest_s}{latest_approx_s}"
    else:
        return None


def _get_name(record) -> str:
    lastn = record.get("last_name")
    firstn = record.get("first_name")

    lastn_s = f"{lastn}" if lastn else ''
    firstn_s = f", {firstn}" if firstn else ''

    return f"{lastn_s}{firstn_s}"
