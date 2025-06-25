import logging
from collections.abc import Generator

from indexer.exceptions import RequiredFieldException
from indexer.helpers.db import mysql_pool
from indexer.helpers.solr import submit_to_solr
from indexer.helpers.utilities import parallelise
from indexer.records.publication import create_publication_index_document

log = logging.getLogger("muscat_indexer")


def _get_publications(cfg: dict) -> Generator[dict, None, None]:
    log.info("Getting list of publications to index")
    conn = mysql_pool.connection()
    curs = conn.cursor()
    dbname: str = cfg["mysql"]["database"]

    sql_query: str = f"""
SELECT pub.id AS pub_id, pub.marc_source AS marc_source, pub.created_at AS created,
        pub.updated_at AS updated,
        JSON_ARRAYAGG(DISTINCT CONCAT('work_', wpubs.work_id)) AS work_ids,
        (SELECT JSON_OBJECT('id', CONCAT('person_', p2.id),
                           'name', p2.full_name,
                           'life_dates', p2.life_dates)
        FROM {dbname}.people p2
        LEFT JOIN {dbname}.publications_to_people sp ON p2.id = sp.person_id
        WHERE sp.publication_id = pub.id
            AND sp.marc_tag = '700'
            AND sp.relator_code = 'att'
        LIMIT 1
       ) AS composer
FROM {dbname}.publications AS pub
    LEFT JOIN {dbname}.works_to_publications wpubs ON pub.id = wpubs.publication_id
    LEFT JOIN {dbname}.works wks ON wpubs.work_id = wks.id
WHERE pub.work_catalogue IN (2, 3)
GROUP BY pub.id
ORDER BY pub.id;"""  # noqa: S608

    curs.execute(sql_query)

    while rows := curs._cursor.fetchmany(cfg["mysql"]["resultsize"]):
        yield rows

    curs.close()
    conn.close()


def index_publications(cfg: dict) -> bool:
    log.info("Indexing Publications")
    publication_groups = _get_publications(cfg)

    parallelise(publication_groups, index_publication_groups, cfg)

    return True


def index_publication_groups(publications: list, cfg: dict) -> bool:
    log.info("Indexing Publication Group")
    records_list: list = []

    for record in publications:
        try:
            doc = create_publication_index_document(record, cfg)
        except RequiredFieldException:
            log.critical("Could not index publication %s", record["id"])
            continue

        records_list.append(doc)

    check = True if cfg["dry"] else submit_to_solr(records_list, cfg)

    if not check:
        log.error("There was an error submitting publications to Solr")

    return check
