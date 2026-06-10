import logging

import pymarc

from indexer.helpers.datelib import process_edtf_date
from indexer.helpers.utilities import (
    get_external_ids,
    external_resource_data,
    get_creator_data,
    get_creator_name,
    get_related_people,
    get_standard_work_titles_data,
    get_titles,
    to_solr_single,
)

log = logging.getLogger("muscat_indexer")


def _get_external_ids(record: pymarc.Record) -> list | None:
    return get_external_ids(record)


def _get_has_incipits(record: pymarc.Record) -> bool:
    return "031" in record


def _get_num_incipits(record: pymarc.Record) -> int:
    return len(record.get_fields("031"))


def _get_earliest_latest_dates(record: pymarc.Record) -> tuple[int | None, int | None]:
    date_statement: str | None = to_solr_single(record, "046", "k")
    if not date_statement:
        return None, None

    edtf_statement: tuple[int | None, int | None] = process_edtf_date(
        date_statement, date_statement
    )

    if edtf_statement == (None, None):
        record_id: str = record["001"].value()
        log.warning(
            "Problem with date statement %s for record %s", date_statement, record_id
        )

    return edtf_statement


def _get_earliest_latest_dates_dtr(previously_computed: list[int] | None) -> str | None:
    # Takes the output of the _get_earliest_latest_dates function and creates a Solr DateRange Statement.
    if not previously_computed:
        return None

    first = previously_computed[0]
    last = previously_computed[1]
    if first is None or last is None:
        return None

    return f"[{first} TO {last}]"


def _get_creator_name(record: pymarc.Record) -> str | None:
    return get_creator_name(record)


def _get_creator_data(record: pymarc.Record) -> list | None:
    return get_creator_data(record)


def _get_external_resources_data(record: pymarc.Record) -> list | None:
    """
    Fetch the external links defined on the record. Note that this will *not* index the links that are linked to
    material group descriptions -- those are handled in the material group indexing section above.
    :param record: A pymarc record
    :return: A list of external links. This will be serialized to a string for storage in Solr.
    """
    if "856" not in record:
        return None

    resources: list = [
        external_resource_data(f)
        for f in record.get_fields("856")
        if f and ("8" not in f or f["8"] != "01")
    ]

    return resources if resources else None


def _get_work_form_data(record: pymarc.Record) -> list[dict] | None:
    if "380" not in record:
        return None

    work_form_fields: list[pymarc.Field] = record.get_fields("380")

    # "Form of work" is tied to the subject headings, so we keep the same JSON structure for
    # the JSON object as we have in the subjects_json in sources.
    ret: list = []
    for field in work_form_fields:
        d = {"id": f"subject_{field['0']}", "subject": field.get("a")}
        # Ensure we remove any None values
        ret.append({k: v for k, v in d.items() if v})

    return ret


def _get_standard_titles_data(record: pymarc.Record) -> list[dict] | None:
    return get_standard_work_titles_data(record)


def _get_alternative_titles_data(record: pymarc.Record) -> list | None:
    return get_titles(record, "430")


def _get_related_people_data(record: pymarc.Record) -> list | None:
    if "500" not in record:
        return None

    work_id: str = f"work_{record['001'].value()}"
    people = get_related_people(
        record, work_id, "work", fields=("500",), ungrouped=True
    )

    return people or None


def _get_related_works_data(record: pymarc.Record) -> list | None:
    if "530" not in record:
        return None

    work_fields: list[pymarc.Field] = record.get_fields("530")

    ret: list = []
    for wf in work_fields:
        d = {
            "work_id": f"work_{wf['0']}",
            "title": wf.get("a", "[Unknown title]"),
            "relationship": wf.get("i"),
            "note": wf.get("g"),
        }
        ret.append({k: v for k, v in d.items() if v})

    return ret


def _get_date_statement(record: pymarc.Record) -> str | None:
    if "046" not in record:
        return None

    date_statement: pymarc.Field = record["046"]

    # If the date statement exists but does not have a
    # $k entry, then it gets skipped.
    try:
        date_value: str = date_statement["k"]
    except KeyError:
        work_id: str = f"work_{record['001'].value()}"
        log.warning("Missing $k entry in 046. Skipping %s.", work_id)
        return None

    date_note: str | None = date_statement.get("z")

    return f"{date_value} ({date_note})" if date_note else f"{date_value}"
