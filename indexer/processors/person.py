import logging
from collections import defaultdict

import pymarc

from indexer.helpers.datelib import process_date_statements
from indexer.helpers.utilities import (
    external_resource_data,
    get_related_people,
    to_solr_multi,
    tokenize_variants,
)

log = logging.getLogger("muscat_indexer")


def _get_external_ids(record: pymarc.Record) -> list | None:
    """Converts DNB and VIAF Ids to a namespaced identifier suitable for expansion later."""
    if "024" not in record:
        return None

    ids: list = record.get_fields("024")

    return [
        f"{idf['2'].lower()}:{idf['a']}"
        for idf in ids
        if (idf and idf.get("2") and idf.get("a"))
    ]


def _get_earliest_latest_dates(record: pymarc.Record) -> list[int] | None:
    date_statements: list | None = to_solr_multi(record, "100", "d")
    if not date_statements:
        return None

    record_id: str = record["001"].value()

    return process_date_statements(date_statements, record_id)


def _get_earliest_latest_dates_dtr(record: pymarc.Record) -> str | None:
    date_range: list[int] | None = _get_earliest_latest_dates(record)
    if not date_range:
        return None

    first = date_range[0]
    last = date_range[1]

    return f"[{first} TO {last}]"


def _get_name_variants(record: pymarc.Record) -> list[str] | None:
    name_variants: list[str] | None = to_solr_multi(record, "400", "a")

    if not name_variants:
        return None

    return tokenize_variants(name_variants)


def _get_name_variant_data(record: pymarc.Record) -> list | None:
    if "400" not in record:
        return None

    name_variants = record.get_fields("400")

    names = defaultdict(list)
    for subf in name_variants:
        if "a" not in subf:
            continue
        # If no $j, then use the "xx" code which will represent "unknown".
        # NB: Some records have "xx" for $j as well, even though it's not an 'official' code.
        category: str = subf.get("j", "xx")
        names[category].append(subf["a"])

    # Sort the variants alphabetically and format as list
    return [{"type": k, "variants": sorted(v)} for k, v in names.items()]


def _get_related_people_data(record: pymarc.Record) -> list | None:
    record_id: str = record["001"].value()
    person_id: str = f"person_{record_id}"
    people: list | None = get_related_people(
        record, person_id, "person", ungrouped=True
    )

    return people


def _get_external_resources_data(record: pymarc.Record) -> list | None:
    """
    Fetch the external links defined on the record.
    :param record: A pymarc record
    :return: A list of external links. This will be serialized to a string for storage in Solr.
    """
    if "856" not in record:
        return None

    return [external_resource_data(f) for f in record.get_fields("856")]


def _get_contributing_projects_data(record: pymarc.Record) -> list | None:
    if "910" not in record:
        return None

    fields: list[pymarc.Field] = record.get_fields("910")

    return [
        {
            "type": "institution",
            "name": f.get("a", "[Unknown name]"),
            "relationship": "contributing_project",
            "institution_id": f"institution_{f['0']}",
            "project_url": f["u"],
        }
        for f in fields
    ]
