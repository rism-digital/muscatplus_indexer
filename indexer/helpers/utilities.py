import logging
import operator
import re
import timeit
import concurrent.futures
from collections import OrderedDict
from functools import wraps
from typing import List, Any, Iterable, Optional, Dict, TypedDict

from indexer.exceptions import RequiredFieldException, MalformedIdentifierException
import pymarc

log = logging.getLogger("muscat_indexer")


def elapsedtime(func):
    """
    Simpler method that just provides the elapsed time for a method call. Used only for the 'main' method
    to provide an elapsed total time for indexing
    :param func:
    :return:
    """
    @wraps(func)
    def timed_f(*args, **kwargs):
        fname = func.__name__
        log.debug(" --- Timing execution for %s ---", fname)
        start = timeit.default_timer()
        ret = func(*args, **kwargs)
        end = timeit.default_timer()
        elapsed: float = end - start

        hours, remainder = divmod(elapsed, 60 * 60)
        minutes, seconds = divmod(remainder, 60)

        log.info("Total time to index %s: %02i:%02i:%02.2f", fname, hours, minutes, seconds)
        return ret

    return timed_f


def parallelise(records: Iterable, func: Any, *args, **kwargs) -> None:
    """
    Given a list of records, this function will parallelise processing of those records. It will
    coalesce the arguments into an array, to be handled by function `func`.

    :param records: A list of records to be processed by `func`. Should be the first argument
    :param func: A function to process and index the records
    :param func: A shared Solr connection object
    :return: None
    """
    with concurrent.futures.ProcessPoolExecutor() as executor:
        futures_list = [executor.submit(func, record, *args, **kwargs)
                        for record in records]

        for f in concurrent.futures.as_completed(futures_list):
            f.result()


def to_solr_single(record: pymarc.Record, field: str, subfield: Optional[str] = None) -> Optional[str]:
    """
    Extracts a single value from the MARC record. Always takes the first instance of the
    tag, and the first instance of the subfield within that tag.

    :param record: A pymarc.Record instance
    :param field: A string indicating the tag that should be extracted.
    :param subfield: An optional subfield. If this is None, then the full value of the field will be returned.
    :return: A string value, or None if not found.
    """
    fields: List[pymarc.Field] = record.get_fields(field)
    if not fields:
        return None

    # If the subfield argument is None, return the whole field value.
    if subfield is None:
        return f"{fields[0].value()}"

    return fields[0][subfield]


def to_solr_single_required(record: pymarc.Record, field: str, subfield: Optional[str] = None) -> str:
    """
    Same operations as the to_solr_single, but raises an exception if the value returned
    is None. This is used for indicating an error in the MARC record where there should
    be a required value but it is missing, and so processing cannot continue.

    :param record: A pymarc.Record instance
    :param field: A string indicating the tag that should be extracted.
    :param subfield: An optional subfield. If this is None, then the full value of the field will be returned.
    :return: A string. A RequiredFieldException will be raised if the field is not found.
    """
    ret: Optional[str] = to_solr_single(record, field, subfield)
    if ret is None:
        record_id: str = record['001']
        log.error(f"%s requires a value, but one was not found for %s.", field, record_id)
        raise RequiredFieldException(f"{field} requires a value, but one was not found for {record_id}.")

    return ret


def to_solr_multi(record: pymarc.Record, field: str, subfield: Optional[str] = None, ungrouped: Optional[bool] = False) -> Optional[List[str]]:
    """
    Returns all the values for a given field and subfield. Extracting this data from the
    field is done by creating an OrderedDict from the keys, and then casting it back to a list. This removes
    duplicates but keeps the original order.

    :param record: A pymarc.Record instance
    :param field: A string indicating the tag that should be extracted
    :param subfield: An optional subfield. If this is not provided, the full value of the field will be returned
        as a MARC string (e.g., "$aFoo$bBar).
    :param ungrouped: If this is True, this function will only return fields that do not have a $8 value. The default is
        False, indicating all fields, regardless of whether they are grouped or not, will be returned.
    :return: A list of strings, or None if not found.
    """
    fields: List[pymarc.Field] = record.get_fields(field)
    if not fields:
        return None

    if subfield is None:
        return list(OrderedDict.fromkeys(f.value() for f in fields if (f and '8' in field is ungrouped)))

    # Treat the subfields as a list of lists, and flatten their values. `get_subfields` returns a list,
    # and we are dealing with a list of fields, so we iterate twice here: Once over the fields, and then
    # over the values in each field.
    # Only return the fields that are not empty.
    if ungrouped:
        return list({val for field in fields for val in field.get_subfields(subfield) if '8' not in field and val})
    return list({val for field in fields for val in field.get_subfields(subfield) if val})


def normalize_id(identifier: str) -> str:
    """
    Muscat IDs come in a wide variety of shapes and sizes, some with leading zeroes, others without.

    This method ensures any identifier is consistent by stripping any leading zeroes off a string. This is done
    by parsing it as an integer, and then returning it as a string again.

    :param identifier: An identifier to normalize
    :return: A normalized identifier
    """

    if not (m := re.match(r"[\d]+", identifier)):
        raise MalformedIdentifierException(f"The identifier {identifier} is not well-formed.")

    return f"{int(m.group())}"


def clean_multivalued(fields: Dict, field_name: str) -> Optional[List[str]]:
    if not fields.get(field_name):
        return None

    return [t for t in fields.get(field_name).splitlines() if t.strip()]


class ExternalResourceDocument(TypedDict, total=False):
    url: Optional[str]
    note: Optional[str]
    link_type: Optional[str]


def external_resource_json(field: pymarc.Field) -> Optional[ExternalResourceDocument]:
    """
    Takes an 856 field and attempts to format a dictionary containing
    the data. Used for adding external links to various places in the indexed records (source, material groups,
    holdings, etc.)

    Due to a misconfiguration, for people the 'Notes' are held in $y at the time of this writing, so we use both fields
    for the notes. See https://github.com/rism-digital/muscat/issues/1081

    :param field: A pymarc.Field. Will return None if the tag is not 856.
    :return: A dictionary of values matching the fields in the 856
    """
    external_resource: ExternalResourceDocument = {}

    if u := field['u']:
        external_resource["url"] = u

    if (n := field['z']) or (n := field['y']):
        external_resource["note"] = n

    if k := field['x']:
        external_resource['link_type'] = k

    return external_resource


def __related_person(field: pymarc.Field, this_id: str, this_type: str, relationship_number: int) -> Dict:
    """
    Generate a related person record. The target of the relationship is given in the person_id field,
    while the source of the relationship is given in the related_id field.

    Empty values and keys will be removed from the response.

    :param field: The pymarc field for the relationship
    :param this_id: The ID of the source record for the relationship
    :param this_id: The type of the source record (institution, person). Enables ID lookups based on type
    :param relationship_number: An integer corresponding to the position of this relationship in the list of all
        relationships for this person. This is because two people can be related in two different ways, so this
        lets us give a unique number to each enumerated relationship.
    :return: A Solr record for the person relationship
    """
    d = {
        "id": f"{relationship_number}",
        "name": field['a'],
        "relationship": field['i'],
        "other_person_id": f"person_{field['0']}",
        "this_id": this_id,
        "this_type": this_type
    }

    return {k: v for k, v in d.items() if v}


def get_related_people(record: pymarc.Record, record_id: str, record_type: str) -> Optional[List[Dict]]:
    people_500: List = record.get_fields('500')
    people_700: List = record.get_fields("700")

    people: List = people_700 + people_500
    if not people:
        return None

    # NB: enumeration starts at 1
    return [__related_person(p, record_id, record_type, i) for i, p in enumerate(people, 1) if p]


def __related_place(field: pymarc.Field, this_id: str, this_type: str, relationship_number: int) -> Dict:
    # Note that as of this writing the places are not controlled by the place authorities,
    # so we don't have a place authority ID to store here.

    # TODO: Fix this to point to the place authority once the IDs are stored in MARC. See
    #   https://github.com/rism-digital/muscat/issues/1080

    d = {
        "id": f"{relationship_number}",
        "name": field["a"],
        "relationship": field["i"],
        "place_id": field["0"],
        "this_id": this_id,
        "this_type": this_type
    }

    # strip any null values from the response so that we can do simple checks for available data by looking for the key.
    return {k: v for k, v in d.items() if v}


def get_related_places(record: pymarc.Record, record_id: str, record_type: str) -> Optional[List[Dict]]:
    places_551: List = record.get_fields("551")
    places_751: List = record.get_fields("751")

    places: List = places_551 + places_751
    if not places:
        return None

    return [__related_place(p, record_id, record_type, i) for i, p in enumerate(places, 1) if p]


def __related_institution(field: pymarc.Field, this_id: str, this_type: str, relationship_number: int) -> Dict:
    d = {
        "id": f"{relationship_number}",
        "name": field["a"],
        "institution_id": f"institution_{field['0']}",
        "this_id": this_id,
        "this_type": this_type
    }

    return {k: v for k, v in d.items() if v}


def get_related_institutions(record: pymarc.Record, record_id: str, record_type: str) -> Optional[List[Dict]]:
    # Due to inconsistencies in authority records, these relationships are held in both these fields.
    institutions_510: List = record.get_fields("510")
    institutions_710: List = record.get_fields("710")

    institutions: List = institutions_710 + institutions_510
    if not institutions:
        return None

    return [__related_institution(p, record_id, record_type, i) for i, p in enumerate(institutions, 1) if p]

