import datetime
import logging
from collections import defaultdict
from typing import Optional

import pymarc

from indexer.helpers.datelib import process_date_statements
from indexer.helpers.utilities import (
    external_resource_data,
    get_related_institutions,
    get_related_people,
    get_related_places,
    normalize_id,
    to_solr_multi,
    tokenize_variants,
)

LATEST_YEAR_IF_MISSING: int = datetime.datetime.now().year
EARLIEST_YEAR_IF_MISSING: int = -2000

log = logging.getLogger("muscat_indexer")


def _get_external_ids(record: pymarc.Record) -> Optional[list]:
    """Converts DNB and VIAF Ids to a namespaced identifier suitable for expansion later."""
    if "024" not in record:
        return None

    ids: list = record.get_fields("024")

    return [
        f"{idf['2'].lower()}:{idf['a']}"
        for idf in ids
        if (idf and idf.get("2") and idf.get("a"))
    ]


def _get_earliest_latest_dates(record: pymarc.Record) -> Optional[list[int]]:
    date_statements: Optional[list] = to_solr_multi(record, "100", "d")
    if not date_statements:
        return None

    record_id: str = normalize_id(record["001"].value())

    return process_date_statements(date_statements, record_id)


def _get_name_variants(record: pymarc.Record) -> Optional[list[str]]:
    name_variants: Optional[list[str]] = to_solr_multi(record, "400", "a")

    if not name_variants:
        return None

    return tokenize_variants(name_variants)


def _get_name_variant_data(record: pymarc.Record) -> Optional[list]:
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


def _get_related_people_data(record: pymarc.Record) -> Optional[list]:
    record_id: str = normalize_id(record["001"].value())
    person_id: str = f"person_{record_id}"
    people: Optional[list] = get_related_people(
        record, person_id, "person", ungrouped=True
    )

    return people


def _get_related_institutions_data(record: pymarc.Record) -> Optional[list]:
    record_id: str = normalize_id(record["001"].value())
    person_id: str = f"person_{record_id}"
    institutions: Optional[list] = get_related_institutions(
        record, person_id, "person", ungrouped=True
    )

    return institutions


def _get_related_places_data(record: pymarc.Record) -> Optional[list]:
    record_id: str = normalize_id(record["001"].value())
    person_id: str = f"person_{record_id}"
    places: Optional[list] = get_related_places(record, person_id, "person")

    return places


def _get_external_resources_data(record: pymarc.Record) -> Optional[list]:
    """
    Fetch the external links defined on the record.
    :param record: A pymarc record
    :return: A list of external links. This will be serialized to a string for storage in Solr.
    """
    if "856" not in record:
        return None

    return [external_resource_data(f) for f in record.get_fields("856")]
