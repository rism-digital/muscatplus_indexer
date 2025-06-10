import pymarc


def _parse_field(line: str) -> pymarc.Field | None:
    # General format: =TAG  ##$afoo$bbar
    tag_value: str = line[1:4]

    # Control fields are those in the <010 range. They do not have
    # subfields, but have the data encoded in them directly.
    if "000" <= tag_value < "010":
        return pymarc.Field(tag=tag_value, data=line[6:])

    if line[8:] == "":
        # A bug in Muscat means some fields are empty.
        return None

    indicators: pymarc.Indicators = pymarc.Indicators(line[6], line[7])
    subfields: list[pymarc.Subfield] = [
        _parse_subf(part)
        # subfields start at 8, but skipping the first $ means we don't have an empty split.
        for part in line[9:].split("$")
        if part  # skips empty strings
    ]

    return pymarc.Field(tag=tag_value, indicators=indicators, subfields=subfields)

def _parse_subf(subf_value: str) -> pymarc.Subfield:
    value: str = subf_value[1:].strip().replace("_DOLLAR_", "$")
    return pymarc.Subfield(subf_value[0], value)


def create_marc(record: str) -> pymarc.Record:
    """
    Creates a pymarc Record from the data stored in Muscat.

    :param record: A raw marc_source record from Muscat
    :return: an instance of a pymarc.Record
    """
    fields: list[pymarc.Field] = [
        pf for line in record.splitlines() if line and line != "" for pf in [_parse_field(line.rstrip("\r\n"))] if pf is not None
    ]

    return pymarc.Record(fields=fields)


def create_marc_list(
    marc_records: str | None, delimiter: str = "\n"
) -> list[pymarc.Record]:
    """
    Will always return a list, potentially an empty one.

    :param marc_records: A string of newline-separated MARC records
    :param delimiter: A string that marks the separator between MARC records. Newline (\n) by default.
    :return: A list of pymarc.Record objects
    """
    return (
        [create_marc(rec.strip()) for rec in marc_records.split(delimiter) if rec]
        if marc_records
        else []
    )
