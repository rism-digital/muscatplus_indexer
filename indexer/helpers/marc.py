import re
import itertools
from typing import Optional, TypedDict, List, Pattern, Set
import pymarc
import logging

MARC_LINE_REGEX: Pattern = re.compile(r'^=(?P<idtag>001)\s{2}(?P<ident>.*)|(?P<tag>\d{3})\s{2}(?P<indicators>[0-9#|]{2})(?P<subfields>.*)', re.S)

MarcSubField = List[str]

log = logging.getLogger("muscat_indexer")


class MarcField(TypedDict):
    tag: str
    indicators: Optional[List]
    subfields: Optional[List[str]]
    data: Optional[str]


def _parse_field(line: str) -> MarcField:
    match = re.search(MARC_LINE_REGEX, line)
    tag_value: str = match.group('tag') or match.group('idtag')
    ind_value: str = match.group('indicators')
    sub_value: str = match.group('subfields') or match.group('ident')

    # Control fields are those in the 001-008 range. They do not have
    # subfields, but have the data encoded in them directly.
    control: bool = tag_value.isdigit() and int(tag_value) < 10
    subfields: Optional[List[str]]

    if not control:
        subf_list: List = sub_value.split("$") if sub_value else []
        parsed_subfields: List[MarcSubField] = [_parse_subf(itm) for itm in subf_list if itm != '']
        subfields = list(itertools.chain.from_iterable(parsed_subfields))
        data = None
    else:
        subfields = None
        data = f"{sub_value}"

    return {
        "tag": tag_value,
        "indicators": list(ind_value) if not control else None,
        "subfields": subfields,
        "data": data
    }


def _parse_subf(subf_value: str) -> List:
    code: str = subf_value[0]
    value: str = subf_value[1:]
    return [code, value]


def create_marc(record: str) -> pymarc.Record:
    """
    Creates a pymarc Record from the data stored in Muscat.

    :param record: A raw marc_source record from Muscat
    :return: an instance of a pymarc.Record
    """
    lines: List = re.split(r"[\r\n]+", record)
    fields: List[MarcField] = [_parse_field(line) for line in lines if line != '']
    r: pymarc.Record = pymarc.Record()

    for field in fields:
        r.add_field(
            pymarc.Field(**field)
        )

    return r


def record_value_lookup(record: pymarc.Record, tag: str, subfield: str) -> Optional[List]:
    """
    Takes a record, tag, and subfield and extracts the string value from that.
    Returns None if the tag or subfield is not found.

    :param record: a pymarc.Record instance
    :param tag: A string representing a MARC tag name
    :param subfield: A string representing the subfield
    :return: A list of subfield values
    """
    fields: List[pymarc.Field] = record.get_fields(tag)
    if not fields:
        return None

    subfields: Set[str] = {f[subfield] for f in fields if subfield in f}

    return list(subfields)


def id_field_lookup(record: pymarc.Record, id_type: str) -> Optional[List]:
    """
    A specialized lookup field for extracting specific ID types from the
    repeatable 024 field. All other fields that do not match the value given
    in the $2 field and the id_type parameter will be ignored.

    Returns None if no 024 field is found with the id field match

    :param record: A pymarc.Record instance
    :param id_type: The value of the $2 field to match
    :return: A list of values from the $a of the 024 that match the id type
    """
    id_fields: List[pymarc.Field] = record.get_fields("024")

    if not id_fields:
        return None

    ids: List = []

    for field in id_fields:
        is_of_type: bool = len(field.get_subfields("2")) > 0 and field.get_subfields("2")[0] == id_type
        # if the record is NOT the type we are looking for, continue.
        if not is_of_type:
            continue

        id_val: List = field.get_subfields("a")
        if not id_val:
            continue

        ids.append(
            id_val[0]
        )

    return ids
