import logging
from collections import defaultdict
from typing import List, Optional

import pymarc

from indexer.helpers.datelib import parse_date_statement
from indexer.helpers.utilities import to_solr_multi, normalize_id, to_solr_single_required, get_related_people, \
    get_related_institutions, get_related_places, external_resource_data

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
    date_statements: Optional[List] = to_solr_multi(record, "100", "d")

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

    earliest_date: int = min(earliest_dates) if earliest_dates else -9999
    latest_date: int = max(latest_dates) if latest_dates else 9999

    # If neither date was parseable, don't pretend we have a date.
    if earliest_date == -9999 and latest_date == 9999:
        return None

    return [earliest_date, latest_date]


def _get_name_variants(record: pymarc.Record) -> Optional[List]:
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
    people = get_related_people(record, person_id, "person", ungrouped=True)
    if not people:
        return None

    return people


def _get_related_institutions_data(record: pymarc.Record) -> Optional[List]:
    person_id: str = f"person_{normalize_id(to_solr_single_required(record, '001'))}"
    institutions = get_related_institutions(record, person_id, "person", ungrouped=True)
    if not institutions:
        return None

    return institutions


def _get_related_places_data(record: pymarc.Record) -> Optional[List]:
    person_id: str = f"person_{normalize_id(to_solr_single_required(record, '001'))}"
    places = get_related_places(record, person_id, "person")
    if not places:
        return None

    return places


def _get_external_resources_data(record: pymarc.Record) -> Optional[List]:
    """
    Fetch the external links defined on the record. Note that this will *not* index the links that are linked to
    material group descriptions -- those are handled in the material group indexing section above.
    :param record: A pymarc record
    :return: A list of external links. This will be serialized to a string for storage in Solr.
    """
    ungrouped_ext_links: List = [external_resource_data(f) for f in record.get_fields("856") if f and '8' not in f]
    if not ungrouped_ext_links:
        return None

    return ungrouped_ext_links
