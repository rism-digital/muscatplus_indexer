import pymarc

from indexer.helpers.utilities import (
    external_resource_data,
    get_catalogue_numbers,
    get_related_institutions,
    get_related_people,
    get_titles,
    to_solr_single,
)


def _get_has_incipits(record: pymarc.Record) -> bool:
    return "031" in record


def _get_num_incipits(record: pymarc.Record) -> int:
    return len(record.get_fields("031"))


def _get_creator_name(record: pymarc.Record) -> str | None:
    if "100" not in record:
        return None

    creator: pymarc.Field = record["100"]
    name: str = creator["a"].strip()
    dates: str = f" ({d})" if (d := creator.get("d")) else ""

    return f"{name}{dates}"


def _get_creator_data(record: pymarc.Record) -> list | None:
    if "100" not in record:
        return None

    record_id: str = record["001"].value()
    source_id: str = f"source_{record_id}"
    creator = get_related_people(record, source_id, "inventory_item", fields=("100",))
    if not creator:
        return None

    creator[0]["relationship"] = "cre"
    return creator


def _is_anonymous_creator(record: pymarc.Record) -> bool:
    if "100" not in record:
        return False

    return to_solr_single(record, "100", "0") == "30004985"


def _get_is_arrangement(record: pymarc.Record) -> bool:
    if "240" not in record:
        return False

    fields: list | None = record.get_fields("240")
    if fields is None:
        return False

    valid_statements: tuple = ("Arr", "arr", "Arrangement")
    # if any 240 field has it, we mark the whole record as an arrangement.
    return any("o" in field and field["o"] in valid_statements for field in fields)


def _get_standard_titles_data(record: pymarc.Record) -> list | None:
    return get_titles(record, "240")


def _get_catalogue_numbers(record: pymarc.Record) -> list | None:
    # Catalogue numbers are spread across a number of fields, including 'opus numbers'
    # (383) and 'catalogue of works' (690), where the catalogue and the catalogue
    # entry are held in different subfields. This function consolidates both of those fields,
    # and unites the separate subfields into a single set of identifiers so that we can search on
    # all of them. The 'get_catalogue_numbers' function depends on having access to the
    # 240 field entry for the correct behaviour, so we also pass this in, even though
    # it doesn't hold any data for the catalogue numbers directly.
    record_tags: set = {f.tag for f in record}
    if {"240", "383", "690"}.isdisjoint(record_tags):
        return None

    title_fields: list = record.get_fields("240")
    if not title_fields:
        return None

    catalogue_record_fields: list[pymarc.Field] = record.get_fields("383", "690")
    catalogue_nums: list = get_catalogue_numbers(
        title_fields[0], catalogue_record_fields
    )

    return catalogue_nums


def _get_subjects(record: pymarc.Record) -> list[dict] | None:
    if "650" not in record:
        return None

    subject_fields: list[pymarc.Field] = record.get_fields("650")

    ret: list = []
    for field in subject_fields:
        d = {"id": f"subject_{field['0']}", "subject": field.get("a")}
        # Ensure we remove any None values
        ret.append({k: v for k, v in d.items() if v})

    return ret


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


def _get_iiif_manifest_uris(record: pymarc.Record) -> list | None:
    if "856" not in record:
        return None

    fields: list[pymarc.Field] = record.get_fields("856")
    return [f["u"] for f in fields if "x" in f and "IIIF" in f["x"]]


def _get_scoring_data(record: pymarc.Record) -> list[dict] | None:
    if "594" not in record:
        return None
    fields: list = record.get_fields("594")

    return [__scoring(i) for i in fields]


def __scoring(field: pymarc.Field) -> dict:
    d = {"voice_instrument": field.get("b"), "number": field.get("c")}

    return {k: v for k, v in d.items() if v}


def _get_additional_titles_data(record: pymarc.Record) -> list | None:
    return get_titles(record, "730")


def _get_related_people_data(record: pymarc.Record) -> list | None:
    if "700" not in record:
        return None

    source_id: str = f"inventory_item_{record['001'].value()}"
    people: list | None = get_related_people(
        record, source_id, "inventory_item", fields=("700",), ungrouped=True
    )

    return people or None


def _get_related_institutions_data(record: pymarc.Record) -> list | None:
    if "710" not in record:
        return None
    source_id: str = f"inventory_item_{record['001'].value()}"
    institutions: list | None = get_related_institutions(
        record, source_id, "inventory_item", fields=("710",)
    )

    return institutions or None


def _get_identified_source_data(record: pymarc.Record) -> list | None:
    if "930" not in record:
        return None

    identified_sources: list[pymarc.Field] = record.get_fields("930")

    out = []
    for field in identified_sources:
        out.append(
            {
                "source_id": f"source_{field['w']}",
                "title": field.get("a"),
                "qualifier": field.get("4"),
            }
        )

    return out


def _has_identified_source(record: pymarc.Record) -> bool:
    return "930" in record
