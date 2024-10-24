from typing import Optional

import pymarc as pymarc

from indexer.helpers.identifiers import country_code_from_siglum
from indexer.helpers.utilities import (
    external_resource_data,
    get_related_institutions,
    get_related_people,
    get_titles,
    normalize_id,
    to_solr_single,
)


def _get_country_code(marc_record: pymarc.Record) -> Optional[str]:
    siglum: Optional[str] = to_solr_single(marc_record, "852", "a")
    if not siglum:
        return None

    return country_code_from_siglum(siglum)


def _get_related_people_data(record: pymarc.Record) -> Optional[list]:
    rism_id: str = normalize_id(record["001"].value())
    holding_id: str = f"holding_{rism_id}"
    return get_related_people(
        record, holding_id, "holding", fields=("700",), ungrouped=True
    )


def _get_related_institutions_data(record: pymarc.Record) -> Optional[list]:
    rism_id: str = normalize_id(record["001"].value())
    holding_id: str = f"holding_{rism_id}"
    return get_related_institutions(record, holding_id, "holding", fields=("710",))


def _get_external_resources_data(record: pymarc.Record) -> Optional[list]:
    """
    Fetch the external links defined on the record. Note that this will *not* index the links that are linked to
    material group descriptions -- those are handled in the material group indexing section above.
    :param record: A pymarc record
    :return: A list of external links. This will be serialized to a string for storage in Solr.
    """
    ungrouped_ext_links: list = [
        external_resource_data(f)
        for f in record.get_fields("856")
        if f and ("8" not in f or f["8"] != "01")
    ]
    if not ungrouped_ext_links:
        return None

    return ungrouped_ext_links


def _has_external_resources(record: pymarc.Record) -> bool:
    """
    Returns 'True' if the record has an 856 field; false if not.
    :param record:
    :return:
    """
    return "856" in record


def _get_standard_titles_data(record: pymarc.Record) -> Optional[list]:
    return get_titles(record, "240")


def _get_holding_titles_data(record: pymarc.Record) -> Optional[dict]:
    if "852" not in record:
        return None

    holding: pymarc.Field = record["852"]
    holding_id = f"institution_{n}" if (n := holding.get("x")) else None

    d = {
        "holding_siglum": holding.get("a"),
        "holding_shelfmark": holding.get("c"),
        "holding_institution": holding.get("e"),
        "holding_institution_id": holding_id,
    }

    return {k: v for k, v in d.items() if v}


# def _get_standard_titles_data(record: pymarc.Record) -> Optional[list]:
#     return get_titles(record, "240")


def _get_iiif_manifest_uris(record: pymarc.Record) -> Optional[list]:
    if "856" not in record:
        return None

    fields: list[pymarc.Field] = record.get_fields("856")
    return [f["u"] for f in fields if "x" in f and "IIIF" in f["x"]]
