import logging
from typing import List, Dict

from indexer.helpers.db import mysql_pool
from indexer.helpers.solr import submit_to_solr
from indexer.records.place import PlaceIndexDocument, create_place_index_document

log = logging.getLogger("muscat_indexer")


def index_places(cfg: Dict) -> bool:
    conn = mysql_pool.connection()
    curs = conn.cursor()

    curs.execute("""SELECT 
        id, 
        name, 
        country, 
        district, 
        notes, 
        alternate_terms, 
        topic, 
        sub_topic 
        FROM muscat_development.places
        WHERE wf_stage = 1;""")

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
