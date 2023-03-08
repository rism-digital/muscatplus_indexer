from typing import Optional

import pymarc
import yaml

from indexer.helpers.marc import create_marc
from indexer.helpers.profiles import process_marc_profile
from indexer.helpers.utilities import normalize_id
from indexer.processors import work as work_processor

work_profile: dict = yaml.full_load(open('profiles/works.yml', 'r'))


def create_work_index_documents(record: dict, cfg: dict) -> list:
    work: str = record['marc_source']
    marc_record: pymarc.Record = create_marc(work)

    rism_id: str = normalize_id(marc_record['001'].value())
    work_id: str = f"work_{rism_id}"

    source_entries: set[str] = {f"source_{n}" for n in d.split("\n") if n and n.strip()} if (d := record.get("source_ids")) else set()

    work_core: dict = {
        "id": work_id,
        "type": "work",
        "sources_ids": list(source_entries),
        "source_count_i": record['source_count']
    }

    additional_fields: dict = process_marc_profile(work_profile, work_id, marc_record, work_processor)
    work_core.update(additional_fields)

    return [work_core]
    