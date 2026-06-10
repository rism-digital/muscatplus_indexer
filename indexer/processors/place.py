import pymarc


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
