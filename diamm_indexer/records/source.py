import logging
from typing import Optional

import orjson

from indexer.helpers.identifiers import (
    COUNTRY_CODE_MAPPING,
    ProjectIdentifiers,
    country_code_from_siglum,
    transform_rism_id,
)

log = logging.getLogger("muscat_indexer")


def create_source_index_documents(record, cfg: dict) -> list[dict]:
    log.debug("Indexing %s", record["shelfmark"])

    composer_names: list
    if "composer_names" in record and record["composer_names"]:
        composer_data = [f.strip() for f in record["composer_names"].split("$") if f]
        composer_components = [x.split("|") for x in composer_data if x]
        composer_names = _get_composer_names(composer_components)
    else:
        composer_names = []

    composer_ids: list
    if "composer_ids" in record and record["composer_ids"]:
        composer_data = [f.strip() for f in record["composer_ids"].split("$") if f]
        composer_components = [x.split("|") for x in composer_data if x]
        composer_ids = [
            transform_rism_id(cid) for cmp in composer_components for cid in cmp
        ]
    else:
        composer_ids = []

    display_label = f"{record['siglum']} {record['shelfmark']}"
    if nm := record.get("name"):
        display_label = f"{display_label} ({nm})"

    general_description: Optional[list] = (
        _get_general_notes(record["general_notes"])
        if record.get("general_notes")
        else None
    )
    holding_institution_id: str = transform_rism_id(record["archive_rism_identifier"])
    country_code: str = country_code_from_siglum(record["siglum"])

    date_ranges: Optional[list]
    if not record["start_date"] and not record["end_date"]:
        date_ranges = None
    elif record["start_date"] and not record["end_date"]:
        date_ranges = [record["start_date"], record["start_date"]]
    elif record["end_date"] and not record["start_date"]:
        date_ranges = [record["end_date"], record["end_date"]]
    else:
        date_ranges = [record["start_date"], record["end_date"]]

    source_record: dict = {
        "id": f"diamm_source_{record['id']}",
        "type": "source",
        "project_s": ProjectIdentifiers.DIAMM,
        "record_uri_sni": f"https://www.diamm.ac.uk/sources/{record['id']}",
        "rism_id": transform_rism_id(record["rism_id"]),
        "diamm_id": record["id"],
        "record_type_s": "collection",
        "source_type_s": "manuscript",
        "content_types_sm": ["musical"],
        "material_source_types_sm": ["Manuscript copy"],
        "material_content_types_sm": ["Notated music"],
        "num_physical_copies_i": 1,  # Only MSS in DIAMM!
        "is_contents_record_b": False,
        "is_collection_record_b": True,
        "is_composite_volume_b": False,
        "has_digitization_b": record[
            "has_images"
        ],  # TODO: Figure out how to fill this out w/ DIAMM images
        "display_label_s": display_label,
        "shelfmark_s": record["shelfmark"],
        "date_statements_sm": [record["date_statement"]],
        "common_name_s": record["name"],
        "date_ranges_im": date_ranges,
        "book_formats_sm": [record["book_format"]],
        "physical_dimensions_s": record["measurements"],
        "people_names_sm": composer_names,
        "source_member_composers_sm": composer_names,
        "related_people_ids": composer_ids,
        "siglum_s": record["siglum"],
        "additional_title_s": record["name"],
        "general_notes_sm": general_description,
        "source_general_notes_smni": general_description,
        "standard_titles_json": orjson.dumps(_get_standard_titles_json(record)).decode(
            "utf-8"
        ),
        "holding_institutions_sm": [record["archive_name"]],
        "holding_institutions_identifiers_sm": _get_full_diamm_holding_identifiers(
            record
        ),
        "holding_institutions_ids": [holding_institution_id],
        "holding_institutions_places_sm": [record["city_name"]],
        "country_codes_sm": [country_code],
        "related_institutions_ids": _get_related_institutions_ids(
            record["related_organizations"]
        ),
        "related_institutions_sm": _get_related_institutions_names(
            record["related_organizations"]
        ),
        "related_institutions_json": orjson.dumps(
            _get_related_institutions_json(record["related_organizations"])
        ).decode("utf-8"),
        "country_names_sm": COUNTRY_CODE_MAPPING.get(country_code, []),
        "minimal_mss_holding_json": orjson.dumps(
            _get_minimal_manuscript_holding_data_diamm(record)
        ).decode("utf-8"),
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
        "institution_id": holding_institution_id,
        "city_s": record["city_name"],
        "external_institution_id": f"diamm_archive_{record['archive_id']}",
        "external_resources_json": orjson.dumps(
            _get_external_institution_resource(record)
        ).decode("utf-8"),
    }

    return [source_record, manuscript_holding]


def _get_composer_names(composer_components: list) -> list:
    ret: list = []
    for c in composer_components:
        (
            lastn,
            firstn,
            earliest,
            earliest_approx,
            latest,
            latest_approx,
            composer_id,
        ) = c

        lastn_s = f"{lastn}" if lastn else ""
        firstn_s = f", {firstn}" if firstn else ""
        earliest_approx_s = "?" if earliest_approx == "t" else ""
        latest_approx_s = "?" if latest_approx == "t" else ""
        earliest_s = f"{earliest}" if earliest and int(earliest) != -1 else ""
        latest_s = f"{latest}" if latest and int(latest) != -1 else ""
        dates_s = (
            f"({earliest_s}{earliest_approx_s}—{latest_s}{latest_approx_s})"
            if earliest_s or latest_s
            else ""
        )
        persn = f"{lastn_s}{firstn_s} {dates_s}"
        ret.append(persn)
    return ret


def _get_standard_titles_json(record) -> list[dict]:
    return [
        {
            "title": n if (n := record.get("name")) else "[No title]",
            "holding_siglum": record["siglum"],
            "holding_shelfmark": record["shelfmark"],
            "source_type": "Manuscript copy",
        }
    ]


def _get_general_notes(notes: str) -> list:
    note_block: list = notes.split("|:|")
    all_notes = [j for n in note_block for j in n.split("\r\n")]
    return list(filter(None, all_notes))


def _get_minimal_manuscript_holding_data_diamm(record) -> list:
    d = {
        "siglum": record["siglum"],
        "holding_institution_name": record["archive_name"],
        "holding_institution_id": f"diamm_institution_{record['archive_id']}",
    }
    return [d]


def _get_full_diamm_holding_identifiers(record) -> list[str]:
    institution_sig = record["siglum"]
    institution_name = record["archive_name"]
    institution_shelfmark = record["shelfmark"]

    return [f"{institution_name} {institution_sig} {institution_shelfmark}"]


def _get_external_institution_resource(record) -> list[dict]:
    return [
        {
            "url": f"https://www.diamm.ac.uk/archives/{record['archive_id']}",
            "link_type": "other",
            "note": f"View {record['archive_name']} record in DIAMM",
        }
    ]


def _get_related_institutions_names(orgs: Optional[str]) -> Optional[list]:
    if not orgs:
        return None

    orgs_raw: list[str] = orgs.split("\n")
    return [o.split("||")[0] for o in orgs_raw]


def _get_related_institutions_ids(orgs: Optional[str]) -> Optional[list]:
    if not orgs:
        return None

    orgs_raw: list[str] = orgs.split("\n")
    return [f"diamm_organization_{o.split('||')[1]}" for o in orgs_raw]


def _get_related_institutions_json(orgs: Optional[str]) -> list[dict]:
    if not orgs:
        return []

    orgs_raw: list[str] = orgs.split("\n")

    orgs_json: list = []
    for org in orgs_raw:
        org_name, org_id = org.split("||")

        d = {
            "id": f"diamm_organization_{org_id}",
            "type": "institution",
            "project_type": "organizations",
            "name": f"{org_name}",
        }

        orgs_json.append(d)

    return orgs_json
