from typing import Optional

import pymarc as pymarc

from indexer.helpers.identifiers import country_code_from_siglum
from indexer.helpers.utilities import (
    to_solr_single,
    external_resource_data,
    get_related_people,
    get_related_institutions, normalize_id,
)


def _get_country_code(marc_record: pymarc.Record) -> Optional[str]:
    siglum: Optional[str] = to_solr_single(marc_record, "852", "a")
    if not siglum:
        return None

    return country_code_from_siglum(siglum)


def _get_related_people_data(record: pymarc.Record) -> Optional[list]:
    rism_id: str = normalize_id(record["001"].value())
    holding_id: str = f"holding_{rism_id}"
    people = get_related_people(record, holding_id, "holding", fields=("700",), ungrouped=True)
    if not people:
        return None

    return people


def _get_related_institutions_data(record: pymarc.Record) -> Optional[list]:
    rism_id: str = normalize_id(record["001"].value())
    holding_id: str = f"holding_{rism_id}"
    institutions = get_related_institutions(record, holding_id, "holding", fields=("710",))
    if not institutions:
        return None

    return institutions


def _get_external_resources_data(record: pymarc.Record) -> Optional[list]:
    """
    Fetch the external links defined on the record. Note that this will *not* index the links that are linked to
    material group descriptions -- those are handled in the material group indexing section above.
    :param record: A pymarc record
    :return: A list of external links. This will be serialized to a string for storage in Solr.
    """
    ungrouped_ext_links: list = [external_resource_data(f) for f in record.get_fields("856") if f and ('8' not in f or f['8'] != "01")]
    if not ungrouped_ext_links:
        return None

    return ungrouped_ext_links


def _has_external_resources(record: pymarc.Record) -> bool:
    """
    Returns 'True' if the record has an 856 field; false if not.
    :param record:
    :return:
    """
    return '856' in record
