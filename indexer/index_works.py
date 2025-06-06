import logging
from collections.abc import Generator

from indexer.exceptions import RequiredFieldException
from indexer.helpers.db import mysql_pool
from indexer.helpers.solr import submit_to_solr
from indexer.helpers.utilities import parallelise
from indexer.records.work import (
    create_work_catalogue_index_document,
    create_work_index_documents,
)

log = logging.getLogger("muscat_indexer")

def _get_work_catalogues(cfg: dict) -> Generator[dict, None, None]:
    log.info("Getting list of work catalogues to index")
    conn = mysql_pool.connection()
    curs = conn.cursor()
    dbname: str = cfg["mysql"]["database"]

    sql_query: str = f"""SELECT pub.id AS pub_id, pub.marc_source AS marc_source, pub.created_at AS created,
                            pub.updated_at AS updated, 
                            JSON_ARRAYAGG(DISTINCT CONCAT('work_', wpubs.work_id)) AS work_ids
                            FROM {dbname}.publications AS pub
                            LEFT JOIN {dbname}.works_to_publications wpubs ON pub.id = wpubs.publication_id
                            LEFT JOIN {dbname}.works wks ON wpubs.work_id = wks.id
                            WHERE pub.work_catalogue = 1 AND pub.wf_stage = 1
                            GROUP BY pub.id
                            ORDER BY pub.id;"""  # noqa: S608

    curs.execute(sql_query)

    while rows := curs._cursor.fetchmany(cfg["mysql"]["resultsize"]):
        yield rows

    curs.close()
    conn.close()



def _get_works(cfg: dict) -> Generator[dict, None, None]:
    log.info("Getting list of works to index")
    conn = mysql_pool.connection()
    curs = conn.cursor()
    dbname: str = cfg["mysql"]["database"]

    sql_query: str = f"""SELECT work.id, work.marc_source, peep.id AS person_id,
                           COUNT(DISTINCT s.id) as source_count, work.created_at AS created, 
                           work.updated_at AS updated,
                           JSON_ARRAYAGG(DISTINCT s.id) as source_ids,
                           JSON_ARRAYAGG(DISTINCT s.marc_source) as source_marc,
                           JSON_ARRAYAGG(DISTINCT pub.marc_source) as publication_marc,
                           JSON_ARRAYAGG(DISTINCT
                                         JSON_OBJECT('id', pub.id,
                                                     'author', pub.author,
                                                     'title', pub.title,
                                                     'journal', pub.journal,
                                                     'date', pub.date,
                                                     'place', pub.place,
                                                     'short_name', pub.short_name)) AS publications,
                           JSON_OBJECT('name', peep.full_name, 'dates', peep.life_dates) AS person_name
                        FROM {dbname}.works AS work
                        LEFT JOIN {dbname}.sources_to_works sw ON work.id = sw.work_id
                        LEFT JOIN {dbname}.sources s ON sw.source_id = s.id
                        LEFT JOIN {dbname}.works_to_publications pw ON work.id = pw.work_id
                        LEFT JOIN {dbname}.publications pub ON pw.publication_id = pub.id
                        LEFT JOIN {dbname}.people peep ON work.person_id = peep.id
                    GROUP BY work.id
                    ORDER BY work.id;"""  # noqa: S608

    curs.execute(sql_query)

    while rows := curs._cursor.fetchmany(cfg["mysql"]["resultsize"]):
        yield rows

    curs.close()
    conn.close()


def index_works(cfg: dict) -> bool:
    log.info("Indexing works")
    work_groups = _get_works(cfg)
    parallelise(work_groups, index_work_groups, cfg)

    return True


def index_work_groups(works: list, cfg: dict) -> bool:
    log.info("Indexing Work Group")
    records_list: list = []

    for record in works:
        try:
            docs = create_work_index_documents(record, cfg)
        except RequiredFieldException:
            log.critical("Could not index work %s", record["id"])
            continue

        log.debug("Appending work document")
        records_list.extend(docs)

    check: bool = True if cfg["dry"] else submit_to_solr(records_list, cfg)

    if not check:
        log.error("There was an error submitting works to Solr")

    return check


def index_work_catalogues(cfg: dict) -> bool:
    log.info("Indexing work catalogues")
    work_catalogue_groups = _get_work_catalogues(cfg)

    parallelise(work_catalogue_groups, index_work_catalogue_groups, cfg)

    return True


def index_work_catalogue_groups(catalogues: list, cfg: dict) -> bool:
    log.info("Indexing Work Catalogue Group")
    records_list: list = []

    for record in catalogues:
        try:
            doc = create_work_catalogue_index_document(record, cfg)
        except RequiredFieldException:
            log.critical("Could not index work catalogue %s", record["id"])
            continue

        records_list.append(doc)

    check = True if cfg["dry"] else submit_to_solr(records_list, cfg)

    if not check:
        log.error("There was an error submitting work catalogues to Solr")

    return check
