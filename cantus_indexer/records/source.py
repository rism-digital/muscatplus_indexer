import logging
import re
from typing import Optional, Pattern

import orjson

from indexer.helpers.datelib import parse_date_statement
from indexer.helpers.identifiers import (
    COUNTRY_CODE_MAPPING,
    ProjectIdentifiers,
    country_code_from_siglum,
    transform_rism_id,
)

log = logging.getLogger("muscat_indexer")


def create_source_index_documents(record, cfg: dict) -> list[dict]:
    log.debug("Indexing %s", record["shelfmark"])

    display_label = f"{record['institution_siglum']} {record['shelfmark']}"

    if inst_sigl := record.get("institution_siglum"):
        country_code: str = country_code_from_siglum(inst_sigl)
    else:
        country_code = "XX"

    inst_identifiers: list[str] = (
        rii.split("\n") if (rii := record.get("institution_rism_ids")) else []
    )
    source_date: str = record.get("source_century", "")
    source_summary: Optional[str] = record.get("source_summary")
    general_note: Optional[str] = record.get("html_source_description")

    source_record: dict = {
        "id": f"cantus_source_{record['id']}",
        "type": "source",
        "project_s": ProjectIdentifiers.CANTUS,
        "record_uri_sni": f"https://cantusdatabase.org/source/{record['id']}",
        "cantus_id": f"{record['id']}",
        "record_type_s": "collection",
        "source_type_s": "manuscript",
        "content_types_sm": ["musical"],
        "material_source_types_sm": ["Manuscript copy"],
        "material_content_types_sm": ["Notated music"],
        "num_physical_copies_i": 1,  # Only MSS in DIAMM!
        "is_contents_record_b": False,
        "is_collection_record_b": True,
        "is_composite_volume_b": False,
        "display_label_s": display_label,
        "shelfmark_s": record["shelfmark"],
        "date_statements_sm": [source_date] if source_date else None,
        "date_ranges_im": _process_dates(source_date),
        "siglum_s": record["institution_siglum"],
        "general_notes_sm": general_note if general_note else None,
        "source_general_notes_smni": general_note if general_note else None,
        "description_summary_sm": source_summary if source_summary else None,
        "standard_titles_json": orjson.dumps(_get_standard_titles_json(record)).decode(
            "utf-8"
        ),
        "holding_institutions_sm": [
            record["institution_name"],
        ],
        "holding_institutions_ids": [
            transform_rism_id(rid) for rid in inst_identifiers
        ],
        "holding_institutions_places_sm": [
            record["institution_city"],
        ],
        "country_codes_sm": [country_code],
        "country_names_sm": COUNTRY_CODE_MAPPING.get(country_code, []),
        "minimal_mss_holding_json": orjson.dumps(
            _get_minimal_manuscript_holding_data_cantus(record)
        ).decode("utf-8"),
        "created": record["created"].strftime("%Y-%m-%dT%H:%M:%SZ"),
        "updated": record["updated"].strftime("%Y-%m-%dT%H:%M:%SZ"),
    }

    manuscript_holding: dict = {
        "id": "",
        "type": "holding",
        "project_s": ProjectIdentifiers.CANTUS,
        "source_id": f"cantus_source_{record['id']}",
        "record_type_s": "collection",
        "source_type_s": "manuscript",
        "content_types_sm": ["musical"],
        "main_title_s": display_label,
        "creator_name_s": None,
        "siglum_s": record["institution_siglum"],
        "shelfmark_s": record["shelfmark"],
        "institution_name_s": record["institution_name"],
        "institution_id": f"cantus_institution_{record['institution_id']}",
        "city_s": record["institution_city"],
        "external_institution_id": f"cantus_institution_{record['institution_id']}",
        "external_resources_json": orjson.dumps(
            _get_external_institution_resource(record)
        ).decode("utf-8"),
        "created": record["created"].strftime("%Y-%m-%dT%H:%M:%SZ"),
        "updated": record["updated"].strftime("%Y-%m-%dT%H:%M:%SZ"),
    }

    return [source_record, manuscript_holding]


def _get_standard_titles_json(record) -> list[dict]:
    return [
        {
            "holding_siglum": record["institution_siglum"],
            "holding_shelfmark": record["shelfmark"],
            "source_type": "Manuscript",
        }
    ]


def _get_minimal_manuscript_holding_data_cantus(record) -> list:
    return [
        {
            "siglum": record["institution_siglum"],
            "holding_institution_name": record["institution_name"],
            "holding_institution_id": f"cantus_institution_{record['institution_id']}",
        }
    ]


DATE_RE: Pattern = re.compile(
    r"(?P<century>\d{2}th century)(?: \(((?P<date_range>\d{3,4}-\d{3,4})|\d{1}.*)\))?"
)


def _process_dates(century: str) -> Optional[tuple[Optional[int], Optional[int]]]:
    if not century:
        return None

    century_components = re.match(DATE_RE, century)
    if not century_components:
        return None

    cents = century_components.groupdict()
    if dr := cents.get("date_range"):
        earliest, latest = dr.split("-")
        return int(earliest), int(latest)

    if cn := cents.get("century"):
        return parse_date_statement(cn)

    return None


def _get_external_institution_resource(record) -> list[dict]:
    return [
        {
            "url": f"https://cantusdatabase.org/institution/{record['institution_id']}",
            "link_type": "other",
            "note": f"View {record['institution_name']} record in Cantus",
        }
    ]
