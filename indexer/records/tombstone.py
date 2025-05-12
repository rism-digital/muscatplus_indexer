from typing import Any


def create_tombstone_index_document(record, cfg: dict) -> dict[str, Any]:
    tombstone_object: dict = {
        "type": "tombstone",
        "record_type_s": record["item_type"].lower(),
        "record_id": record["item_id"],
        "display_name_s": record["name"],
        "removed_dt": record["deleted"].strftime("%Y-%m-%dT%H:%M:%SZ"),
    }

    return tombstone_object
