from typing import List
import logging
import ujson
import pysolr

log = logging.getLogger("muscat_indexer")

solr_conn: pysolr.Solr = pysolr.Solr("http://localhost:8983/solr/muscat-plus-ingest",
                                     decoder=ujson, always_commit=False, timeout=120)


def submit_to_solr(records: List) -> bool:
    """
    Submits a set of records to a Solr server.

    :param records: A list of Solr records to index
    :return: True if successful, false if not.
    """
    log.debug("Indexing records to Solr")
    try:
        solr_conn.add(records, commit=False)
    except pysolr.SolrError as e:
        log.error("Could not index to Solr. %s", e)
        log.error(records)
        return False

    return True
