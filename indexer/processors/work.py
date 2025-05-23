import pymarc

from indexer.helpers.utilities import (
    external_resource_data,
    get_related_people,
    normalize_id,
)


def _get_has_incipits(record: pymarc.Record) -> bool:
    return "031" in record


def _get_num_incipits(record: pymarc.Record) -> int:
    return len(record.get_fields("031"))

def _get_creator_name(record: pymarc.Record) -> str | None:
    if "100" not in record:
        return None

    creator: pymarc.Field = record["100"]
    name: str = creator.get("a", "").strip()
    dates: str = f" ({d})" if (d := creator.get("d")) else ""

    return f"{name}{dates}"


def _get_creator_data(record: pymarc.Record) -> list | None:
    if "100" not in record:
        return None

    record_id: str = normalize_id(record["001"].value())
    source_id: str = f"source_{record_id}"
    creator = get_related_people(record, source_id, "source", fields=("100",))
    if not creator:
        return None

    creator[0]["relationship"] = "cre"
    return creator

def _get_external_resources_data(record: pymarc.Record) -> list | None:
    """
    Fetch the external links defined on the record. Note that this will *not* index the links that are linked to
    material group descriptions -- those are handled in the material group indexing section above.
    :param record: A pymarc record
    :return: A list of external links. This will be serialized to a string for storage in Solr.
    """
    if "856" not in record:
        return None

    resources: list = [
        external_resource_data(f)
        for f in record.get_fields("856")
        if f and ("8" not in f or f["8"] != "01")
    ]

    return resources if resources else None
