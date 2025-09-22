import orjson
import pymarc
import yaml

from indexer.helpers.marc import create_marc
from indexer.helpers.profiles import process_marc_profile
from indexer.helpers.utilities import (
    get_bibliographic_references_json,
)
from indexer.processors import work as work_processor
from indexer.records.incipits import get_work_incipits

work_profile: dict = yaml.full_load(open("profiles/works.yml"))  # noqa: SIM115


def create_work_index_documents(record: dict, cfg: dict) -> list:
    work: str = record["marc_source"]
    marc_record: pymarc.Record = create_marc(work)
    rism_id: str = marc_record["001"].value()
    work_id: str = f"work_{rism_id}"

    publications: list = (
        orjson.loads(d) if (d := record.get("publication_entries")) else []
    )
    attached_sources: list = orjson.loads(s) if (s := record["sources"]) else []
    source_count: int = len(attached_sources)
    source_entries: list[str] = [ss["id"] for ss in attached_sources]
    works_catalogue: list[dict] | None = get_bibliographic_references_json(
        marc_record, "690", publications
    )

    secondary_works_catalogue: list[dict] = get_bibliographic_references_json(
        marc_record, "691", publications
    ) or []

    secondary_works_catalogue_titles: list[str] = [f"{w["short_name"]} {w.get("pages")}" for w in secondary_works_catalogue]

    work_core: dict = {
        "id": work_id,
        "type": "work",
        "rism_id": rism_id,
        "full_rism_id": f"works/{rism_id}",
        "sources_ids": source_entries,
        "source_count_i": source_count,
        "works_catalogue_json": orjson.dumps(works_catalogue[0]).decode("utf-8")
        if works_catalogue
        else None,
        "secondary_works_catalogue_json": orjson.dumps(
            secondary_works_catalogue
        ).decode("utf-8")
        if secondary_works_catalogue
        else None,
        "secondary_catalogue_numbers_sm": secondary_works_catalogue_titles,
        "created": record["created"].strftime("%Y-%m-%dT%H:%M:%SZ"),
        "updated": record["updated"].strftime("%Y-%m-%dT%H:%M:%SZ"),
    }

    additional_fields: dict = process_marc_profile(
        work_profile, work_id, marc_record, work_processor
    )
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
