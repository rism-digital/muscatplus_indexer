from typing import Optional

import pymarc

from indexer.helpers.utilities import normalize_id, get_related_people


def _get_creator_name(record: pymarc.Record) -> Optional[str]:
    if "100" not in record:
        return None

    creator: pymarc.Field = record["100"]
    name: str = creator.get("a", "").strip()
    dates: str = f" ({d})" if (d := creator.get("d")) else ""

    return f"{name}{dates}"


def _get_creator_data(record: pymarc.Record) -> Optional[list]:
    if "100" not in record:
        return None

    record_id: str = normalize_id(record["001"].value())
    source_id: str = f"source_{record_id}"
    creator = get_related_people(record, source_id, "source", fields=("100",))
    if not creator:
        return None

    creator[0]["relationship"] = "cre"
    return creator
