import orjson
import pymarc
import yaml

from indexer.helpers.marc import create_marc
from indexer.helpers.profiles import process_marc_profile
from indexer.helpers.utilities import (
    get_bibliographic_references_json,
    normalize_id,
)
from indexer.processors import work as work_processor
from indexer.processors import work_catalogue as work_catalogue_processor
from indexer.records.incipits import get_work_incipits

work_profile: dict = yaml.full_load(open("profiles/works.yml"))  # noqa: SIM115
work_catalogues_profile: dict = yaml.full_load(open("profiles/work_catalogues.yml"))  # noqa: SIM115


def create_work_catalogue_index_document(record: dict, cfg: dict) -> dict:
    catalogue: str = record["marc_source"]
    marc_record: pymarc.Record = create_marc(catalogue)
    rism_id: str = normalize_id(marc_record["001"].value())
    work_catalogue_id: str = f"publication_{rism_id}"

    work_ids_raw: list = w.split("\n") if (w := record["work_ids"]) else []
    work_ids: list = [f"work_{wid}" for wid in work_ids_raw]

    catalogue_core: dict = {
        "id": work_catalogue_id,
        "type": "publication",
        "rism_id": rism_id,
        "full_rism_id": f"publications/{rism_id}",
        "is_work_catalogue_b": True,
        "work_ids": work_ids,
        "works_count_i": len(work_ids)
    }

    additional_fields: dict = process_marc_profile(
        work_catalogues_profile, work_catalogue_id, marc_record, work_catalogue_processor
    )

    catalogue_core.update(additional_fields)

    return catalogue_core




def create_work_index_documents(record: dict, cfg: dict) -> list:
    work: str = record["marc_source"]
    marc_record: pymarc.Record = create_marc(work)
    rism_id: str = normalize_id(marc_record["001"].value())
    work_id: str = f"work_{rism_id}"

    publications: list = (
        orjson.loads(d)
        if (d := record.get("publications"))
        else []
    )
    source_entries: set[str] = (
        {f"source_{n}" for n in d.split("\n") if n and n.strip()}
        if (d := record.get("source_ids"))
        else set()
    )
    works_catalogue: list[dict] | None = get_bibliographic_references_json(
        marc_record, "690", publications
    )

    secondary_works_catalogue: list[dict] | None = get_bibliographic_references_json(
        marc_record, "691", publications
    )

    work_core: dict = {
        "id": work_id,
        "type": "work",
        "rism_id": rism_id,
        "full_rism_id": f"works/{rism_id}",
        "sources_ids": list(source_entries),
        "source_count_i": record["source_count"],
        "works_catalogue_json": orjson.dumps(works_catalogue).decode("utf-8")
        if works_catalogue
        else None,
        "secondary_works_catalogue_json": orjson.dumps(secondary_works_catalogue).decode("utf-8")
        if secondary_works_catalogue
        else None
    }

    additional_fields: dict = process_marc_profile(
        work_profile, work_id, marc_record, work_processor
    )
    work_core.update(additional_fields)

    creator_name: str | None = additional_fields.get("creator_name_s")
    work_title: str | None = additional_fields.get("standard_title_s")

    incipits: list = get_work_incipits(marc_record, work_title, creator_name) or []
    res = [work_core]
    res.extend(incipits)

    return res
