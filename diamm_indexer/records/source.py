import logging

import orjson

from diamm_indexer.helpers.identifiers import transform_rism_id
from indexer.helpers.identifiers import ProjectIdentifiers

log = logging.getLogger("muscat_indexer")


def update_rism_source_document(record, cfg: dict) -> dict:
    document_id: str = transform_rism_id(record.get("rism_id"))

    return {
        "id": document_id,
        "diamm_b": {"set": True},
    }


def create_source_index_documents(record, cfg: dict) -> list[dict]:
    log.debug("Indexing %s", record['shelfmark'])

    composer_names: list
    if 'composer_names' in record and record['composer_names']:
        composer_data = [f.strip() for f in record['composer_names'].split("$") if f]
        composer_components = [x.split('|') for x in composer_data if x]
        composer_names = _get_composer_names(composer_components)
    else:
        composer_names = []

    display_label = f"{record['siglum']} {record['shelfmark']}"
    if nm := record.get("name"):
        display_label = f"{display_label} ({nm})"

    source_record = {
        "id": f"diamm_source_{record['id']}",
        "type": "source",
        "project_s": ProjectIdentifiers.DIAMM,
        "resource_uri_sni": f"https://www.diamm.ac.uk/sources/{record['id']}",
        "rism_id": transform_rism_id(record['rism_id']),
        "diamm_id": record['id'],
        "record_type_s": "collection",
        "source_type_s": "manuscript",
        "content_types_sm": ["musical"],
        "display_label_s": display_label,
        "shelfmark_s": record['shelfmark'],
        "date_statements_sm": [record["date_statement"]],
        "common_name_s": record['name'],
        "date_ranges_im": [record['start_date'], record['end_date']],
        "people_names_sm": composer_names,
        "siglum_s": record['siglum'],
        "source_title_s": record["name"],
        "standard_titles_json": orjson.dumps(_get_standard_titles_json(record)).decode("utf-8"),
        "created": record["created"].strftime("%Y-%m-%dT%H:%M:%SZ"),
        "updated": record["updated"].strftime("%Y-%m-%dT%H:%M:%SZ"),
    }

    manuscript_holding: dict = {
        "id": f"diamm_holding_{record['id']}",
        "type": "holding",
        "project_s": ProjectIdentifiers.DIAMM,
        "source_id": f"diamm_source_{record['id']}",
        "record_type_s": "collection",
        "source_type_s": "manuscript",
        "content_types_sm": ["musical"],
        "main_title_s": display_label,
        "creator_name_s": None,
        "siglum_s": record["siglum"],
        "shelfmark_s": record["shelfmark"],
        "institution_name_s": record["archive_name"],
        "institution_id": f"diamm_institution_{record['archive_id']}",
    }

    return [source_record, manuscript_holding]


def _get_composer_names(composer_components: list) -> list:
    ret: list = []
    for c in composer_components:
        try:
            (lastn, firstn, earliest, earliest_approx, latest, latest_approx, composer_id) = c
        except ValueError as e:
            print(c)
            raise

        lastn_s = f"{lastn}" if lastn else ''
        firstn_s = f", {firstn}" if firstn else ''
        earliest_approx_s = f"?" if earliest_approx == 't' else ''
        latest_approx_s = f"?" if latest_approx == 't' else ''
        earliest_s = f"{earliest}" if earliest and int(earliest) != -1 else ''
        latest_s = f"{latest}" if latest and int(latest) != -1 else ''
        dates_s = f"({earliest_s}{earliest_approx_s}—{latest_s}{latest_approx_s})" if earliest_s or latest_s else ''
        persn = f"{lastn_s}{firstn_s} {dates_s}"
        ret.append(persn)
    return ret


def _get_standard_titles_json(record) -> list[dict]:
    return [{
        "title": n if (n := record.get('name')) else "[No title]",
        "holding_siglum": record["siglum"],
        "holding_shelfmark": record["shelfmark"],
        "source_type": "Manuscript copy"
    }]
