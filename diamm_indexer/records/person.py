import logging
from typing import Optional

import orjson

from diamm_indexer.helpers.utilities import get_related_sources_json
from indexer.helpers.identifiers import ProjectIdentifiers

log = logging.getLogger("muscat_indexer")


def create_person_index_document(record, cfg: dict) -> list[dict]:
    related_sources: list = get_related_sources_json(record['related_sources'])
    copied_sources: list = get_related_sources_json(record['copied_sources'])
    all_related_sources = related_sources + copied_sources
    num_related_sources = len(all_related_sources)

    d = {
        "id": f"diamm_person_{record['id']}",
        "type": "person",
        "project_s": ProjectIdentifiers.DIAMM,
        "record_uri_sni": f"https://www.diamm.ac.uk/people/{record['id']}",
        "name_s": get_name(record),
        "last_name_s": record.get("last_name"),
        "first_name_s": record.get("first_name"),
        "earliest_year_i": record.get("earliest_year"),
        "latest_year_i": record.get("latest_year"),
        "date_statement_s": get_date_statement(record),
        "related_sources_json": orjson.dumps(all_related_sources).decode('utf-8') if all_related_sources else None,
        "total_sources_i": num_related_sources,
    }

    return [d]


def get_date_statement(record) -> Optional[str]:
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


def get_name(record) -> str:
    lastn = record.get("last_name")
    firstn = record.get("first_name")

    lastn_s = f"{lastn}" if lastn else ''
    firstn_s = f", {firstn}" if firstn else ''

    return f"{lastn_s}{firstn_s}"
