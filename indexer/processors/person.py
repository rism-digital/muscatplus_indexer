import datetime
import logging
from collections import defaultdict
from typing import List, Optional

import pymarc

from indexer.helpers.datelib import parse_date_statement
from indexer.helpers.utilities import to_solr_multi, normalize_id, to_solr_single_required, get_related_people, \
    get_related_institutions, get_related_places, external_resource_data, tokenize_variants

LATEST_YEAR_IF_MISSING: int = datetime.datetime.now().year
EARLIEST_YEAR_IF_MISSING: int = -2000


log = logging.getLogger("muscat_indexer")


def _get_external_ids(record: pymarc.Record) -> Optional[List]:
    """Converts DNB and VIAF Ids to a namespaced identifier suitable for expansion later. """
    ids: List = record.get_fields('024')
    if not ids:
        return None

    return [f"{idf['2'].lower()}:{idf['a']}" for idf in ids if (idf and idf['2'])]


def _get_earliest_latest_dates(record: pymarc.Record) -> Optional[List[int]]:
    earliest_dates: List[int] = []
    latest_dates: List[int] = []
    date_statements: Optional[List] = to_solr_multi(record, "100", "d", ungrouped=True)

    # if no date statement, return an empty dictionary. This allows us to keep a consistent return type
    # since a call to `.update()` with an empty dictionary won't do anything.
    if not date_statements:
        return None

    for statement in date_statements:
        try:
            earliest, latest = parse_date_statement(statement)
        except Exception as e:  # noqa
            # The breadth of errors mean we could spend all day catching things, so in this case we use
            # a blanket exception catch and then log the statement to be fixed so that we might fix it later.
            log.error("Error parsing date statement %s: %s", statement, e)
            raise

        if earliest:
            earliest_dates.append(earliest)

        if latest:
            latest_dates.append(latest)

    earliest_date: int = min(earliest_dates) if earliest_dates else EARLIEST_YEAR_IF_MISSING
    latest_date: int = max(latest_dates) if latest_dates else LATEST_YEAR_IF_MISSING

    # If neither date was parseable, don't pretend we have a date.
    if earliest_date == EARLIEST_YEAR_IF_MISSING and latest_date == LATEST_YEAR_IF_MISSING:
        return None

    return [earliest_date, latest_date]


def _get_name_variants(record: pymarc.Record) -> Optional[List[str]]:
    name_variants: Optional[List[str]] = to_solr_multi(record, "400", "a", ungrouped=True)

    if not name_variants:
        return None

    return tokenize_variants(name_variants)


def _get_name_variant_data(record: pymarc.Record) -> Optional[List]:
    name_variants = record.get_fields("400")
    if not name_variants:
        return None

    names = defaultdict(list)
    for subf in name_variants:
        if not (n := subf["a"]):
            continue
        # If no $j, then use the "xx" code which will represent "unknown".
        # NB: Some records have "xx" for $j as well, even though it's not an 'official' code.
        category: str = subf["j"] or "xx"
        names[category].append(n)

    # Sort the variants alphabetically and format as list
    name_variants: List = [{"type": k, "variants": sorted(v)} for k, v in names.items()]

    return name_variants


def _get_related_people_data(record: pymarc.Record) -> Optional[List]:
    person_id: str = f"person_{normalize_id(to_solr_single_required(record, '001'))}"
    people: Optional[List] = get_related_people(record, person_id, "person", ungrouped=True)

    return people


def _get_related_institutions_data(record: pymarc.Record) -> Optional[List]:
    person_id: str = f"person_{normalize_id(to_solr_single_required(record, '001'))}"
    institutions: Optional[List] = get_related_institutions(record, person_id, "person", ungrouped=True)

    return institutions


def _get_related_places_data(record: pymarc.Record) -> Optional[List]:
    person_id: str = f"person_{normalize_id(to_solr_single_required(record, '001'))}"
    places: Optional[List] = get_related_places(record, person_id, "person")

    return places


def _get_external_resources_data(record: pymarc.Record) -> Optional[List]:
    """
    Fetch the external links defined on the record.
    :param record: A pymarc record
    :return: A list of external links. This will be serialized to a string for storage in Solr.
    """
    ext_links: List = [external_resource_data(f) for f in record.get_fields("856")]
    if not ext_links:
        return None

    return ext_links
