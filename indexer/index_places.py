import logging
from collections import deque
from collections.abc import Generator

from indexer.helpers.db import mysql_pool
from indexer.helpers.solr import submit_to_solr
from indexer.helpers.utilities import parallelise
from indexer.records.place import create_place_index_document

log = logging.getLogger("muscat_indexer")


def _get_place_groups(cfg: dict) -> Generator[dict]:
    conn = mysql_pool.connection()
    curs = conn.cursor()
    dbname: str = cfg["mysql"]["database"]

    id_where_clause: str = ""
    if "id" in cfg:
        id_where_clause = f"AND p.id = {cfg['id']}"

    sql_statement = f"""SELECT
                    p.id AS id,
                    p.name AS name,
                    p.country AS country,
                    p.district AS district,
                    p.marc_source AS marc_source,
                    p.notes AS notes,
                    p.alternate_terms AS alternate_terms,
                    (SELECT COUNT(DISTINCT(sp.source_id)) FROM {dbname}.sources_to_places AS sp WHERE sp.place_id = p.id) AS sources_count,
                    (SELECT COUNT(DISTINCT(pp.person_id)) FROM {dbname}.people_to_places AS pp WHERE pp.place_id = p.id) AS people_count,
                    (SELECT COUNT(DISTINCT(ip.institution_id)) FROM {dbname}.institutions_to_places AS ip WHERE ip.place_id = p.id) AS institutions_count,
                    (SELECT COUNT(DISTINCT(hp.holding_id)) FROM {dbname}.holdings_to_places AS hp WHERE hp.place_id = p.id) AS holdings_count
                FROM {dbname}.places AS p;"""  # noqa: S608

    curs.execute(sql_statement)

    while rows := curs._cursor.fetchmany(cfg["mysql"]["resultsize"]):
        yield rows

    curs.close()
    conn.close()


def index_places(cfg: dict) -> bool:
    log.info("Indexing Places")
    place_groups = _get_place_groups(cfg)
    parallelise(place_groups, index_place_groups, cfg)

    return True


def index_place_groups(places: list, cfg: dict) -> bool:
    records_to_index: deque = deque()

    for place in places:
        doc: dict = create_place_index_document(place, cfg)
        records_to_index.append(doc)

    check: bool = True if cfg["dry"] else submit_to_solr(list(records_to_index), cfg)

    if not check:
        log.error("There was an error submitting places to Solr")
        return False

    return True
