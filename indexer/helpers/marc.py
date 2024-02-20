from typing import Optional

import pymarc


def _parse_field(line: str) -> pymarc.Field:
    # General format: =TAG  ##$afoo$bbar
    tag_value: str = line[1:4]

    # Control fields are those in the <010 range. They do not have
    # subfields, but have the data encoded in them directly.
    control: bool = tag_value.isdigit() and int(tag_value) < 10
    if control:
        return pymarc.Field(tag=tag_value, data=line[6:])

    ind_value: str = line[6:8]
    indicators: list = list(ind_value)
    sub_value: str = line[9:]
    subf_list: list = sub_value.split("$") if sub_value else []
    subfields: list[pymarc.Subfield] = [_parse_subf(itm) for itm in subf_list if itm != '']
    return pymarc.Field(tag=tag_value, indicators=indicators, subfields=subfields)


def _parse_subf(subf_value: str) -> pymarc.Subfield:
    code: str = subf_value[0]
    value: str = subf_value[1:].strip()

    if "_DOLLAR_" in value:
        value = value.replace("_DOLLAR_", "$")

    return pymarc.Subfield(code, value)


def create_marc(record: str) -> pymarc.Record:
    """
    Creates a pymarc Record from the data stored in Muscat.

    :param record: A raw marc_source record from Muscat
    :return: an instance of a pymarc.Record
    """
    lines: list = record.split("\n")
    fields: list[pymarc.Field] = [_parse_field(line) for line in lines if line and line != '']
    p_record: pymarc.Record = pymarc.Record()
    p_record.add_field(*fields)

    return p_record


def create_marc_list(marc_records: Optional[str]) -> list[pymarc.Record]:
    """
    Will always return a list, potentially an empty one.

    :param marc_records: A string of newline-separated MARC records
    :return: A list of pymarc.Record objects
    """
    return [create_marc(rec.strip()) for rec in marc_records.split("\n") if rec] if marc_records else []
