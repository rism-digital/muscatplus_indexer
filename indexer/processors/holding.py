from typing import Optional, List

import pymarc as pymarc

from indexer.helpers.identifiers import country_code_from_siglum
from indexer.helpers.utilities import to_solr_single, external_resource_data


def _get_country_code(marc_record: pymarc.Record) -> Optional[str]:
    siglum: Optional[str] = to_solr_single(marc_record, "852", "a")
    if not siglum:
        return None

    return country_code_from_siglum(siglum)


def _get_external_resources_data(record: pymarc.Record) -> Optional[List]:
    """
    Fetch the external links defined on the record. Note that this will *not* index the links that are linked to
    material group descriptions -- those are handled in the material group indexing section above.
    :param record: A pymarc record
    :return: A list of external links. This will be serialized to a string for storage in Solr.
    """
    ungrouped_ext_links: List = [external_resource_data(f) for f in record.get_fields("856") if f and ('8' not in f or f['8'] != "0")]
    if not ungrouped_ext_links:
        return None

    return ungrouped_ext_links
