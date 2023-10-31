from typing import Optional

import orjson
import pymarc
import yaml

from indexer.helpers.marc import create_marc
from indexer.helpers.profiles import process_marc_profile
from indexer.helpers.utilities import (
    normalize_id,
    get_bibliographic_references_json,
    get_creator_name,
)
from indexer.processors import work as work_processor

work_profile: dict = yaml.full_load(open("profiles/works.yml", "r"))


def create_work_index_documents(record: dict, cfg: dict) -> list:
    work: str = record["marc_source"]
    marc_record: pymarc.Record = create_marc(work)

    rism_id: str = normalize_id(marc_record["001"].value())
    work_id: str = f"work_{rism_id}"

    publication_entries: list = (
        list({n.strip() for n in d.split("|~|") if n and n.strip()})
        if (d := record.get("publication_entries"))
        else []
    )
    source_entries: set[str] = (
        {f"source_{n}" for n in d.split("\n") if n and n.strip()}
        if (d := record.get("source_ids"))
        else set()
    )
    works_catalogue: Optional[list[dict]] = get_bibliographic_references_json(
        marc_record, "690", publication_entries
    )

    work_core: dict = {
        "id": work_id,
        "type": "work",
        "sources_ids": list(source_entries),
        "source_count_i": record["source_count"],
        "works_catalogue_json": orjson.dumps(works_catalogue).decode("utf-8")
        if works_catalogue
        else None,
    }

    additional_fields: dict = process_marc_profile(
        work_profile, work_id, marc_record, work_processor
    )
    work_core.update(additional_fields)

    return [work_core]
