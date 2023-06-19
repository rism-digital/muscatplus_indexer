import logging

log = logging.getLogger("muscat_indexer")


def create_source_index_documents(record, cfg) -> dict:
    log.debug("Indexing %s", record['shelfmark'])

    d = {
        "id": f"diamm_source_{record['id']}",
        "type": "source",
        "db_s": "diamm",
        "record_type_s": "collection",
        "source_type_s": "manuscript",
        "content_types_sm": None,  # FIXME: Figure out what this should be
        "shelfmark_s": record['shelfmark'],
        "date_statement_s": record["date_statement"],
        "date_ranges_im": [record['start_date'], record['end_date']],
        "siglum_s": record['siglum'],
        "source_title_s": record["name"],
        "created": record["created"].strftime("%Y-%m-%dT%H:%M:%SZ"),
        "updated": record["updated"].strftime("%Y-%m-%dT%H:%M:%SZ"),
    }

    return d
