import pymarc

from indexer.helpers.parse_dates import process_date_statements
from indexer.helpers.utilities import (
    external_resource_data,
    get_creator_data,
    get_creator_name,
    get_related_institutions,
    get_related_people,
    to_solr_multi,
)


def _get_creator_name(record: pymarc.Record) -> str | None:
    return get_creator_name(record)


def _get_creator_data(record: pymarc.Record) -> list | None:
    return get_creator_data(record, "publication", "aut")


def _get_earliest_latest_dates(record: pymarc.Record) -> list[int] | None:
    date_statements: list | None = to_solr_multi(record, "260", "c")
    if not date_statements:
        return None

    record_id: str = record["001"].value()

    return process_date_statements(date_statements, record_id, "publication")


def _get_earliest_latest_dates_dtr(previously_computed: list[int] | None) -> str | None:
    # Takes the output of the _get_earliest_latest_dates function and creates a Solr DateRange Statement.
    if not previously_computed:
        return None

    first = previously_computed[0]
    last = previously_computed[1]

    return f"[{first} TO {last}]"


def _get_related_people_data(record: pymarc.Record) -> list | None:
    if "700" not in record:
        return None

    publication_id: str = f"publication_{record['001'].value()}"
    people = get_related_people(
        record, publication_id, "publication", fields=("700",), ungrouped=True
    )

    return people or None


def _get_related_institutions_data(record: pymarc.Record) -> list | None:
    if "710" not in record:
        return None
    publication_id: str = f"publication_{record['001'].value()}"
    institutions = get_related_institutions(
        record, publication_id, "publication", fields=("710",)
    )

    return institutions or None


def _get_series_statement_data(record: pymarc.Record) -> list | None:
    if "760" not in record:
        return None

    statements: list[pymarc.Field] = record.get_fields("760")
    out: list = []

    for stmt in statements:
        d = {
            "title": stmt.get("t"),
            "volumes": ", ".join(vn for vn in stmt.get_subfields("g") if vn),
        }
        out.append({k: v for k, v in d.items() if v})

    return out


def _get_external_resources_data(record: pymarc.Record) -> list | None:
    """
    Fetch the external links defined on the record. Note that this will *not* index the links that are linked to
    material group descriptions -- those are handled in the material group indexing section above.
    :param record: A pymarc record
    :return: A list of external links. This will be serialized to a string for storage in Solr.
    """
    if "856" not in record:
        return None

    resources: list = [external_resource_data(f) for f in record.get_fields("856") if f]

    return resources if resources else None


def _get_iiif_manifest_uris(record: pymarc.Record) -> list | None:
    if "856" not in record:
        return None

    fields: list[pymarc.Field] = record.get_fields("856")
    return [f["u"] for f in fields if "x" in f and "IIIF" in f["x"]]


def _get_has_external_resources(record: pymarc.Record) -> bool:
    return "856" in record
