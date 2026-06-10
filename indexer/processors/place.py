import pymarc

from indexer.helpers.utilities import get_external_ids


def _get_external_ids(record: pymarc.Record) -> list | None:
    return get_external_ids(record)
