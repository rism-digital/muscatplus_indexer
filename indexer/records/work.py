import orjson
import pymarc
import yaml

from indexer.helpers.bibliography import (
    get_bibliographic_references_json,
)
from indexer.helpers.marc import create_marc
from indexer.helpers.profiles import compile_marc_profile, process_marc_profile
from indexer.processors import work as work_processor
from indexer.records.incipits import get_work_incipits

raw_work_profile: dict = yaml.full_load(open("profiles/works.yml"))  # noqa: SIM115
work_profile = compile_marc_profile(raw_work_profile, work_processor)


def create_work_index_documents(record: dict, cfg: dict) -> list:
    work: str = record["marc_source"]
    marc_record: pymarc.Record = create_marc(work)
    rism_id: str = marc_record["001"].value()
    work_id: str = f"work_{rism_id}"

    publications: list = record.get("publication_entries") or []
    attached_sources: list = record["sources"] or []
    source_count: int = len(attached_sources)
    works_catalogue: list[dict] = (
        get_bibliographic_references_json(marc_record, "690", publications) or []
    )

    secondary_works_catalogue: list[dict] = (
        get_bibliographic_references_json(marc_record, "691", publications) or []
    )

    works_catalogue_titles: list[str] = [
        f"{w['short_name']} {w.get('pages')}" for w in works_catalogue
    ]

    secondary_works_catalogue_titles: list[str] = [
        f"{w['short_name']} {w.get('pages')}" for w in secondary_works_catalogue
    ]

    source_data_entries: list = record["source_data_found"] or []

    source_data_found: list[dict] | None = get_bibliographic_references_json(
        marc_record, "670", source_data_entries, control_subf="w"
    )
    source_data_found_json = (
        orjson.dumps(source_data_found).decode("utf-8") if source_data_found else []
    )

    work_core: dict = {
        "id": work_id,
        "type": "work",
        "rism_id": rism_id,
        "full_rism_id": f"works/{rism_id}",
        "sources_ids": attached_sources,
        "source_count_i": source_count,
        "works_catalogue_json": orjson.dumps(works_catalogue).decode("utf-8")
        if works_catalogue
        else None,
        "catalogue_numbers_sm": works_catalogue_titles,
        "secondary_works_catalogue_json": orjson.dumps(
            secondary_works_catalogue
        ).decode("utf-8")
        if secondary_works_catalogue
        else None,
        "secondary_catalogue_numbers_sm": secondary_works_catalogue_titles,
        "source_data_found_json": source_data_found_json,
        "created": record["created"].strftime("%Y-%m-%dT%H:%M:%SZ"),
        "updated": record["updated"].strftime("%Y-%m-%dT%H:%M:%SZ"),
    }

    additional_fields: dict = process_marc_profile(work_profile, work_id, marc_record)
    work_core.update(additional_fields)

    creator_name: str | None = additional_fields.get("creator_name_s")
    work_title: str | None = additional_fields.get("standard_title_s")

    incipits: list = get_work_incipits(marc_record, work_title, creator_name) or []
    if incipits:
        # Store the first incipit on the work record so we can render it without needing to
        # look it up.
        first_one = incipits[0]
        ext = {
            "work_num_s": first_one.get("work_num_s"),
            "has_notation_b": first_one.get("has_notation_b", False),
            "original_pae_sni": first_one.get("original_pae_sni"),
            "is_mensural_b": first_one.get("is_mensural_b", False),
        }
        work_core.update(ext)

    res = [work_core]
    res.extend(incipits)

    return res
