import csv
import logging.config

import MySQLdb  # type: ignore
import pymarc
import yaml
from dbutils.pooled_db import PooledDB  # type: ignore
from MySQLdb.cursors import SSDictCursor  # type: ignore

RECORD_TYPE = "source"
RECORD_TYPE_PLURAL = "sources"


idx_config: dict = yaml.full_load(open("../index_config.yml"))

log = logging.getLogger("incipit_checker")

config: dict = {
    "user": idx_config["mysql"]["username"],
    "password": idx_config["mysql"]["password"],
    "db": idx_config["mysql"]["database"],
    "host": idx_config["mysql"]["server"],
}


mysql_connection = MySQLdb.connect(**config, cursorclass=SSDictCursor)

mysql_pool = PooledDB(
    **config,
    creator=MySQLdb,
    cursorclass=SSDictCursor,
    maxconnections=6,
    charset="utf8mb4",
    use_unicode=True,
)


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
        pf
        for line in record.splitlines()
        if line and line != ""
        for pf in [_parse_field(line.rstrip("\r\n"))]
        if pf is not None
    ]

    return pymarc.Record(fields=fields)


def _get_work_number(field: pymarc.Field, document_id: str) -> tuple[str, bool]:
    work_num = field.get("a", "x")
    mvt_num = field.get("b", "x")
    inc_num = field.get("c", "x")

    some_error = False
    if not work_num.isdigit() or not mvt_num.isdigit() or not inc_num.isdigit():
        log.error(
            "Incipit numbering is not correct for %s (%s.%s.%s)",
            document_id,
            work_num,
            mvt_num,
            inc_num,
        )
        some_error = True

    work_number: str = f"{work_num}.{mvt_num}.{inc_num}"

    if work_number == "x.x.x":
        log.warning("Bad incipit number for %s", document_id)
        some_error = True

    return work_number, some_error


if __name__ == "__main__":
    query = f"""
    SELECT r.id AS id, r.marc_source AS marc_source, r.wf_stage AS wf_stage
    FROM muscat_development.{RECORD_TYPE_PLURAL} AS r
    ORDER BY r.id desc;
    """  # noqa: S608

    conn = mysql_pool.connection()
    curs = conn.cursor()

    curs.execute(query)

    errors: list[dict] = []

    while record := curs._cursor.fetchone():
        marc_source = record["marc_source"]
        marc_record = create_marc(marc_source)
        record_id = marc_record["001"].value()
        if "031" not in marc_record:
            continue

        record_id = marc_record["001"].value()
        log.info("Processing %s", record_id)
        incipits: list[pymarc.Field] = marc_record.get_fields("031")
        siglum_f: pymarc.Field | None = marc_record.get("852")
        siglum: str = siglum_f.get("a", "") if siglum_f else ""

        source_of_description: pymarc.Field | None = marc_record.get("588")
        cataloguing_copy: str = (
            source_of_description.get("a", "") if source_of_description else ""
        )

        pubstate = "published" if record["wf_stage"] == 1 else "unpublished"

        incipit_numbers: set = set()

        for incipit in incipits:
            work_number, number_error = _get_work_number(incipit, record_id)
            if number_error:
                errors.append(
                    {
                        "record": record_id,
                        "url": f"https://muscat.rism.info/admin/{RECORD_TYPE_PLURAL}/{record_id}",
                        "work_number": work_number,
                        "siglum": siglum,
                        "type": "format",
                        "state": pubstate,
                        "cataloguing_copy": cataloguing_copy,
                    }
                )

            if work_number in incipit_numbers:
                log.error("Duplicate incipit number %s for %s", work_number, record_id)
                errors.append(
                    {
                        "record": record_id,
                        "url": f"https://muscat.rism.info/admin/{RECORD_TYPE_PLURAL}/{record_id}",
                        "work_number": work_number,
                        "siglum": siglum,
                        "type": "duplicate",
                        "state": pubstate,
                        "cataloguing_copy": cataloguing_copy,
                    }
                )
            incipit_numbers.add(work_number)

    with open(f"{RECORD_TYPE}-incipit-errors.csv", "w", newline="") as csvfile:
        fieldnames = [
            "record",
            "url",
            "work_number",
            "siglum",
            "type",
            "state",
            "cataloguing_copy",
        ]
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(errors)
