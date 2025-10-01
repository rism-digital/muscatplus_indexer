import logging

import pymarc

from indexer.helpers.utilities import (
    external_resource_data,
    get_related_people,
    get_titles,
)

log = logging.getLogger("muscat_indexer")


def _get_external_ids(record: pymarc.Record) -> list | None:
    """Converts DNB and VIAF Ids to a namespaced identifier suitable for expansion later."""
    if "024" not in record:
        return None

    ids: list = record.get_fields("024")

    return [
        f"{idf['2'].lower()}:{idf['a']}"
        for idf in ids
        if (idf and idf.get("2") and idf.get("a"))
    ]


def _get_has_incipits(record: pymarc.Record) -> bool:
    return "031" in record


def _get_num_incipits(record: pymarc.Record) -> int:
    return len(record.get_fields("031"))


def _get_creator_name(record: pymarc.Record) -> str | None:
    if "100" not in record:
        return None

    creator: pymarc.Field = record["100"]
    name: str = creator.get("a", "").strip()
    dates: str = f" ({d})" if (d := creator.get("d")) else ""

    return f"{name}{dates}"


def _get_creator_data(record: pymarc.Record) -> list | None:
    if "100" not in record:
        return None

    record_id: str = record["001"].value()
    source_id: str = f"source_{record_id}"
    creator = get_related_people(record, source_id, "source", fields=("100",))
    if not creator:
        return None

    creator[0]["relationship"] = "cre"
    return creator


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


# def _validate_edtf_date(
#     value: str, doc_id: str, marc_field: str | None, marc_subfield: str | None
# ) -> bool:
#     is_valid: bool = is_valid_edtf(value)
#     if is_valid:
#         return True
#
#     fixed_date = convert_to_edtf(value)
#     if fixed_date == value:
#         # couldn't be fixed.
#         log.warning("%s could not be fixed on %s", value, doc_id)
#         return False
#
#     now_is_valid: bool = is_valid_edtf(fixed_date)
#     if now_is_valid:
#         log.critical(
#             '"%s" was fixed to "%s" on "%s", "%s", "%s"',
#             value,
#             fixed_date,
#             doc_id,
#             marc_field,
#             marc_subfield,
#         )
#         return True
#
#     log.warning(
#         "%s was fixed to %s on %s, but was still not valid EDTF.",
#         value,
#         fixed_date,
#         doc_id,
#     )
#     return False


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
    if "130" not in record:
        return None

    titles: list[pymarc.Field] = record.get_fields("130")

    out: list = []
    for title in titles:
        d = {
            "title": title.get("a"),
            "key_mode": title.get("r"),
            "scoring_summary": title.get("m"),
        }
        out.append({k: v for k, v in d.items() if v})

    return out


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
