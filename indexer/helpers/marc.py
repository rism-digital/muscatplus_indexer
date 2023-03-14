import logging
from typing import Optional, TypedDict, Iterator

import pymarc

MarcSubField = list[str]

log = logging.getLogger("muscat_indexer")


class MarcField(TypedDict):
    tag: str
    indicators: Optional[list]
    subfields: Optional[list[pymarc.CodedSubfield]]
    data: Optional[str]


def _parse_field(line: str) -> MarcField:
    # General format: =TAG  ##$afoo$bbar
    tag_value: str = line[1:4]

    # Control fields are those in the <010 range. They do not have
    # subfields, but have the data encoded in them directly.
    control: bool = tag_value.isdigit() and int(tag_value) < 10
    subfields: Optional[list[pymarc.CodedSubfield]]
    indicators: Optional[list[str]]
    if control:
        subfields = None
        indicators = None
        data = line[6:]
    else:
        ind_value: str = line[6:8]
        indicators: list = list(ind_value)
        sub_value: str = line[9:]
        subf_list: list = sub_value.split("$") if sub_value else []
        subfields: list[pymarc.CodedSubfield] = [_parse_subf(itm) for itm in subf_list if itm != '']
        data = None

    return {
        "tag": tag_value,
        "indicators": indicators,
        "subfields": subfields,
        "data": data
    }


def _parse_subf(subf_value: str) -> pymarc.CodedSubfield:
    code: str = subf_value[0]
    value: str = subf_value[1:].strip()

    if "_DOLLAR_" in value:
        value = value.replace("_DOLLAR_", "$")

    return pymarc.CodedSubfield(code, value)


def create_marc(record: str) -> pymarc.Record:
    """
    Creates a pymarc Record from the data stored in Muscat.

    :param record: A raw marc_source record from Muscat
    :return: an instance of a pymarc.Record
    """
    lines: list = record.split("\n")
    fields: Iterator[MarcField] = (_parse_field(line) for line in lines if line and line != '')
    r: pymarc.Record = pymarc.Record()

    all_fields = (pymarc.Field(**field) for field in fields)
    r.add_field(*all_fields)

    return r


def create_marc_list(marc_records: Optional[str]) -> list[pymarc.Record]:
    """
    Will always return a list, potentially an empty one.

    :param marc_records: A string of newline-separated MARC records
    :return: A list of pymarc.Record objects
    """
    return [create_marc(rec.strip()) for rec in marc_records.split("\n") if rec] if marc_records else []
