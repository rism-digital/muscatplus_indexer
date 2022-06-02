from typing import Optional

import logging
import pymarc

from indexer.helpers.identifiers import country_code_from_siglum, KALLIOPE_MAPPING, COUNTRY_CODE_MAPPING, \
    ISO3166_TO_SIGLUM_MAPPING
from indexer.helpers.utilities import to_solr_single_required, to_solr_single, normalize_id, get_related_people, \
    get_related_institutions, get_related_places, external_resource_data


log = logging.getLogger("muscat_indexer")


def _get_external_ids(record: pymarc.Record) -> Optional[list]:
    """Converts DNB and VIAF Ids to a namespaced identifier suitable for expansion later. """
    ids: list = record.get_fields('024')
    if not ids:
        return None

    return [f"{idf['2'].lower()}:{idf['a']}" for idf in ids if (idf and idf['2'])]


# This is a multivalued field with a single value so that we can use the same field name (country_codes_sm)
# as sources.
def _get_country_codes(record: pymarc.Record) -> Optional[list[str]]:
    return [_get_country_code(record)]


def _get_country_code(record: pymarc.Record) -> Optional[str]:
    if '110' not in record or '043' not in record:
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
    if record['034'] and (lon := record['034']['d']) and (lat := record['034']['f']):
        # Check the values of the lat/lon
        try:
            _ = float(lon)
            _ = float(lat)
        except ValueError:
            log.warning("Problem with the following values lat,lon %s,%s: %s", lat, lon, record["001"].value())
            return None

        return f"{lat},{lon}"

    return None


def _get_institution_types(record: pymarc.Record) -> list[str]:
    all_institution_type_fields: list[pymarc.Field] = record.get_fields("368")
    all_types: set = set()

    # gather all the different values
    for itfield in all_institution_type_fields:
        field_labels: list[str] = itfield.get_subfields("a")
        # Splits on any semicolon, strips any extraneous space from the split strings, and flattens the result into
        # a single list of all values, and ignores any values that evaluate to 'None'.
        split_field_labels: list[str] = [item.strip() for sublist in field_labels if sublist for item in sublist.split(";") if item]
        all_types.update(split_field_labels)

    mapped: set = set()
    # If the type matches a key in the kalliope mapping, add the value from the mapping.
    # Otherwise, add the type to the mapped list directly.
    for institution_type in all_types:
        if institution_type in KALLIOPE_MAPPING:
            mapped.add(KALLIOPE_MAPPING[institution_type])
        else:
            mapped.add(institution_type)

    return list(mapped)


def _get_related_people_data(record: pymarc.Record) -> Optional[list]:
    institution_id: str = f"institution_{normalize_id(record['001'].value())}"
    people: Optional[list] = get_related_people(record, institution_id, "institution", ungrouped=True)

    return people


def _get_related_institutions_data(record: pymarc.Record) -> Optional[list]:
    institution_id: str = f"institution_{normalize_id(record['001'].value())}"
    institutions: Optional[list] = get_related_institutions(record, institution_id, "institution", fields=('710',))

    return institutions


def _get_related_places_data(record: pymarc.Record) -> Optional[list]:
    institution_id: str = f"institution_{normalize_id(record['001'].value())}"
    places: Optional[list] = get_related_places(record, institution_id, "institution")

    return places


def _get_external_resources_data(record: pymarc.Record) -> Optional[list]:
    """
    Fetch the external links defined on the record.
    :param record: A pymarc record
    :return: A list of external links. This will be serialized to a string for storage in Solr.
    """
    ext_links: list = [external_resource_data(f) for f in record.get_fields("856")]
    if not ext_links:
        return None

    return ext_links
