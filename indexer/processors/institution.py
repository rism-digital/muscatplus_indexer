import logging
from typing import Optional

import pymarc

from indexer.helpers.identifiers import (
    COUNTRY_CODE_MAPPING,
    ISO3166_TO_SIGLUM_MAPPING,
    country_code_from_siglum,
)
from indexer.helpers.utilities import (
    external_resource_data,
    get_related_institutions,
    get_related_people,
    get_related_places,
    normalize_id,
    to_solr_single,
)

log = logging.getLogger("muscat_indexer")


def _get_external_ids(record: pymarc.Record) -> Optional[list]:
    """Converts DNB and VIAF Ids to a namespaced identifier suitable for expansion later."""
    if "024" not in record:
        return None
    ids: list[pymarc.Field] = record.get_fields("024")

    return [
        f"{idf['2'].lower()}:{idf['a']}"
        for idf in ids
        if (idf and idf.get("2") and idf.get("a"))
    ]


# This is a multivalued field with a single value so that we can use the same field name (country_codes_sm)
# as sources.
def _get_country_codes(record: pymarc.Record) -> Optional[list[str]]:
    return [_get_country_code(record)]


def _get_country_code(record: pymarc.Record) -> Optional[str]:
    if "110" not in record or "043" not in record:
        return None

    siglum: Optional[str] = to_solr_single(record, "110", "g")

    # If we have a siglum, prefer this.
    if siglum:
        return country_code_from_siglum(siglum)

    iso_country_code: Optional[str] = to_solr_single(record, "043", "c")
    if iso_country_code:
        # look up the siglum prefix from the country code mapping
        # Also returns None if not found
        return ISO3166_TO_SIGLUM_MAPPING.get(iso_country_code)

    # If the above fails, we can't assign a country.
    return None


def _get_country_names(record: pymarc.Record) -> Optional[list]:
    country_code = _get_country_code(record)
    if not country_code:
        return None

    # will also return None if the country code is not found.
    return COUNTRY_CODE_MAPPING.get(country_code)


def _get_location(record: pymarc.Record) -> Optional[str]:
    if "034" not in record:
        return None

    location_field: pymarc.Field = record["034"]
    if (lon := location_field.get("d")) and (lat := location_field.get("f")):
        try:
            _ = float(lon)
            _ = float(lat)
        except ValueError:
            log.warning(
                "Problem with the following values lat,lon %s,%s: %s",
                lat,
                lon,
                record["001"].value(),
            )
            return None

        return f"{lat},{lon}"
    return None


def _get_related_people_data(record: pymarc.Record) -> Optional[list]:
    record_id: str = normalize_id(record["001"].value())
    institution_id: str = f"institution_{record_id}"
    people: Optional[list] = get_related_people(
        record, institution_id, "institution", ungrouped=True
    )

    return people


def _get_related_institutions_data(record: pymarc.Record) -> Optional[list]:
    record_id: str = normalize_id(record["001"].value())
    institution_id: str = f"institution_{record_id}"
    institutions: Optional[list] = get_related_institutions(
        record, institution_id, "institution", fields=("710",)
    )

    return institutions


def _get_related_places_data(record: pymarc.Record) -> Optional[list]:
    record_id: str = normalize_id(record["001"].value())
    institution_id: str = f"institution_{record_id}"
    places: Optional[list] = get_related_places(record, institution_id, "institution")

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


def _address(address_field: pymarc.Field) -> Optional[dict]:
    d = {
        "street": address_field.get_subfields("a"),  # list
        "city": address_field.get_subfields("b"),  # list
        "county": address_field.get_subfields("c"),
        "country": address_field.get_subfields("d"),
        "postcode": address_field.get_subfields("e"),
        "email": address_field.get_subfields("m"),
        "website": address_field.get_subfields("u"),
        "note": address_field.get_subfields("z"),
    }

    return {k: v for k, v in d.items() if v}


def _get_addresses_data(record: pymarc.Record) -> Optional[list]:
    if "371" not in record:
        return None

    addresses: list[pymarc.Field] = record.get_fields("371")

    return [_address(ent) for ent in addresses]
