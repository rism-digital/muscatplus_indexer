import orjson
import pymarc
import yaml

from indexer.helpers.marc import create_marc
from indexer.helpers.profiles import process_marc_profile
from indexer.helpers.utilities import get_related_sources
from indexer.processors import inventory_item as inventory_item_processor
from indexer.records.incipits import get_inventory_item_incipits

with open("profiles/inventory_items.yml") as pi:
    inventory_items_profile: dict = yaml.full_load(pi)


def create_inventory_item_index_document(record: dict, cfg: dict) -> list[dict]:
    marc_record: pymarc.Record = create_marc(record["marc_source"])
    item_id = f"inventory_item_{record['id']}"
    rism_id = f"{record['id']}"
    source_id = f"source_{record['source_id']}"

    related_sources = None
    if t := record.get("related_sources"):
        related_source_fields: list[pymarc.Field] = marc_record.get_fields("787")
        source_list: list = orjson.loads(t)
        related_sources = get_related_sources(
            source_list, related_source_fields, source_id
        )

    related_sources_json = (
        orjson.dumps(related_sources).decode("utf-8") if related_sources else None
    )

    inventory_item: dict = {
        "id": item_id,
        "type": "inventory_item",
        "rism_id": rism_id,
        "source_id": source_id,
        "main_title_s": record.get("title", "[No title]"),
        "related_sources_json": related_sources_json,
        "source_order_i": record.get("source_order", 0),
        "created": record["created"].strftime("%Y-%m-%dT%H:%M:%SZ"),
        "updated": record["updated"].strftime("%Y-%m-%dT%H:%M:%SZ"),
    }

    additional_fields: dict = process_marc_profile(
        inventory_items_profile, item_id, marc_record, inventory_item_processor
    )
    inventory_item.update(additional_fields)

    main_title = inventory_item["main_title_s"]
    creator_name = inventory_item.get("creator_name_s", "[Unknown creator]")

    incipits = (
        get_inventory_item_incipits(marc_record, source_id, main_title, creator_name)
        or []
    )

    res: list = [inventory_item]
    res.extend(incipits)

    return res
