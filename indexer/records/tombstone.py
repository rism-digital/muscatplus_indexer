from typing import Any


def create_tombstone_index_document(record, cfg: dict) -> dict[str, Any]:
    record_type_id: str = record["item_type"].lower()
    record_id: str = record["item_id"]

    tombstone_object: dict = {
        "id": f"tombstone_{record_type_id}_{record_id}",
        "type": "tombstone",
        "record_type_s": record_type_id,
        "record_id": record_id,
        "display_name_s": record["name"],
        "removed_dt": record["deleted"].strftime("%Y-%m-%dT%H:%M:%SZ"),
    }

    return tombstone_object
