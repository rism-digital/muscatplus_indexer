import logging
from typing import List, Dict

from indexer.helpers.db import mysql_pool
from indexer.helpers.solr import submit_to_solr
from indexer.records.place import PlaceIndexDocument, create_place_index_document

log = logging.getLogger("muscat_indexer")


def index_places(cfg: Dict) -> bool:
    log.info("Indexing places")
    conn = mysql_pool.connection()
    curs = conn.cursor()
    dbname: str = cfg['mysql']['database']

    id_where_clause: str = ""
    if "id" in cfg:
        id_where_clause = f"AND p.id = {cfg['id']}"

    curs.execute(f"""SELECT
                p.id AS id,
                p.name AS name,
                p.country AS country,
                p.district AS district,
                p.notes AS notes,
                p.alternate_terms AS alternate_terms,
                p.topic AS topic,
                p.sub_topic AS sub_topic,
                (SELECT COUNT(DISTINCT(sp.source_id)) FROM {dbname}.sources_to_places AS sp WHERE sp.place_id = p.id) AS sources_count,
                (SELECT COUNT(DISTINCT(pp.person_id)) FROM {dbname}.people_to_places AS pp WHERE pp.place_id = p.id) AS people_count,
                (SELECT COUNT(DISTINCT(ip.institution_id)) FROM {dbname}.institutions_to_places AS ip WHERE ip.place_id = p.id) AS institutions_count,
                (SELECT COUNT(DISTINCT(hp.holding_id)) FROM {dbname}.holdings_to_places AS hp WHERE hp.place_id = p.id) AS holdings_count
            FROM {dbname}.places AS p
            WHERE
                (SELECT COUNT(sp.source_id) FROM {dbname}.sources_to_places AS sp WHERE sp.place_id = p.id) > 0 OR
                (SELECT COUNT(pp.person_id) FROM {dbname}.people_to_places AS pp WHERE pp.place_id = p.id) > 0 OR
                (SELECT COUNT(ip.institution_id) FROM {dbname}.institutions_to_places AS ip WHERE ip.place_id = p.id) > 0 OR
                (SELECT COUNT(hp.holding_id) FROM {dbname}.holdings_to_places AS hp WHERE hp.place_id = p.id) > 0 
                {id_where_clause};""")

    all_places: List[Dict] = curs._cursor.fetchall()

    records_to_index: List = []
    for place in all_places:
        doc: PlaceIndexDocument = create_place_index_document(place)
        records_to_index.append(doc)

    check: bool = submit_to_solr(records_to_index)

    if not check:
        log.error("There was an error submitting places to Solr")
        return False

    return True
