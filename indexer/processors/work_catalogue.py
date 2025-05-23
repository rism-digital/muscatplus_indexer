import pymarc

from indexer.helpers.utilities import (
    get_related_institutions,
    get_related_people,
    normalize_id,
)


def _get_creator_name(record: pymarc.Record) -> str | None:
    if "100" not in record:
        return None

    creator: pymarc.Field = record["100"]
    name: str = creator["a"].strip()
    dates: str = f" ({d})" if (d := creator.get("d")) else ""

    return f"{name}{dates}"


def _get_creator_data(record: pymarc.Record) -> list | None:
    if "100" not in record:
        return None

    record_id: str = normalize_id(record["001"].value())
    publication_id: str = f"publication_{record_id}"
    creator = get_related_people(record, publication_id, "publication", fields=("100",))
    if not creator:
        return None

    creator[0]["relationship"] = "cre"
    return creator

def _get_related_people_data(record: pymarc.Record) -> list | None:
    if "700" not in record:
        return None

    publication_id: str = f"publication_{record['001'].value()}"
    people = get_related_people(
        record, publication_id, "publication", fields=("700",), ungrouped=True
    )

    return people or None

def _get_related_institutions_data(record: pymarc.Record) -> list | None:
    if "710" not in record:
        return None
    publication_id: str = f"publication_{record['001'].value()}"
    institutions = get_related_institutions(
        record, publication_id, "publication", fields=("710",)
    )

    return institutions or None
