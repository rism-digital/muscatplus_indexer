import gc
import logging
from collections import deque
from typing import Generator

from indexer.exceptions import RequiredFieldException
from indexer.helpers.db import mysql_pool
from indexer.helpers.solr import submit_to_solr
from indexer.helpers.utilities import parallelise
from indexer.records.source import create_source_index_documents

log = logging.getLogger("muscat_indexer")


def _get_sources(cfg: dict) -> Generator[dict, None, None]:
    log.info("Getting list of sources to index")
    conn = mysql_pool.connection()
    curs = conn.cursor()
    dbname: str = cfg['mysql']['database']

    id_where_clause: str = ""
    if "id" in cfg:
        id_where_clause = f"AND child.id = {cfg['id']}"

    sql_query: str = f"""SELECT child.id AS id, child.title AS title, child.std_title AS std_title,
        child.source_id AS source_id, child.marc_source AS marc_source, child.composer AS creator_name,
        child.created_at AS created, child.updated_at AS updated, parent.marc_source AS parent_marc_source,
        child.record_type AS record_type, parent.std_title AS parent_title,
        parent.record_type AS parent_record_type,
        (SELECT COUNT(hh.id) FROM {dbname}.holdings AS hh WHERE hh.source_id = child.id) AS holdings_count,
        (SELECT COUNT(ss.id) FROM {dbname}.sources AS ss WHERE ss.source_id = child.id) as child_count,
        (SELECT GROUP_CONCAT(DISTINCT srt.record_type SEPARATOR ',') FROM {dbname}.sources AS srt WHERE srt.source_id = child.id AND srt.source_id IS NOT NULL GROUP BY srt.source_id) AS child_record_types,
        (SELECT GROUP_CONCAT(DISTINCT parent_srt.record_type SEPARATOR ',') FROM {dbname}.sources AS parent_srt WHERE parent_srt.source_id = parent.id) AS parent_child_record_types,
        (SELECT GROUP_CONCAT(DISTINCT srm.composer SEPARATOR '\n') FROM {dbname}.sources AS srm WHERE srm.source_id IS NOT NULL AND srm.source_id = child.id) AS child_composer_list,
        (SELECT GROUP_CONCAT(DISTINCT ins.place SEPARATOR '|') FROM {dbname}.sources_to_institutions ssi LEFT JOIN {dbname}.institutions ins ON ssi.institution_id = ins.id WHERE child.id = ssi.source_id) AS institution_places,
        GROUP_CONCAT(h.marc_source SEPARATOR '\n') AS holdings_marc,
        GROUP_CONCAT(hp.marc_source SEPARATOR '\n') as parent_holdings_marc,
        GROUP_CONCAT(DISTINCT h.lib_siglum SEPARATOR '\n') AS holdings_org,
        GROUP_CONCAT(DISTINCT hp.lib_siglum SEPARATOR '\n') AS parent_holdings_org,
        GROUP_CONCAT(DISTINCT CONCAT_WS('', p.full_name, NULLIF( CONCAT(" (", p.life_dates, ")"), '')) SEPARATOR '\n') AS people_names,
        GROUP_CONCAT(DISTINCT p.alternate_names SEPARATOR '\n') AS alt_people_names,
        GROUP_CONCAT(DISTINCT st.alternate_terms SEPARATOR '\n') AS alt_standard_terms,
        GROUP_CONCAT(DISTINCT p.id SEPARATOR '\n') AS people_ids
        FROM {dbname}.sources AS child
        LEFT JOIN {dbname}.sources AS parent ON parent.id = child.source_id
        LEFT JOIN {dbname}.holdings h on child.id = h.source_id
        LEFT JOIN {dbname}.holdings hp on parent.id = hp.source_id
        LEFT JOIN {dbname}.sources_to_people sp on sp.source_id = child.id
        LEFT JOIN {dbname}.people p on sp.person_id = p.id
        LEFT JOIN {dbname}.sources_to_standard_terms sst on sst.source_id = child.id
        LEFT JOIN {dbname}.standard_terms st ON sst.standard_term_id = st.id
        WHERE child.wf_stage = 1 {id_where_clause}
        GROUP BY child.id
        ORDER BY child.id desc;"""

    curs.execute(sql_query)

    while rows := curs._cursor.fetchmany(cfg['mysql']['resultsize']):  # noqa
        yield rows

    curs.close()
    conn.close()


def index_sources(cfg: dict) -> bool:
    log.info("Indexing sources")
    source_groups = _get_sources(cfg)
    parallelise(source_groups, index_source_groups, cfg)

    return True


def index_source_groups(sources: list, cfg: dict) -> bool:
    log.info("Indexing Source Group")
    records_to_index: deque = deque()

    for record in sources:
        try:
            docs = create_source_index_documents(record, cfg)
        except RequiredFieldException:
            log.critical("Could not index source %s", record["id"])
            continue
        log.debug("Appending source document")
        records_to_index.extend(docs)

    records_list: list = list(records_to_index)
    check: bool = submit_to_solr(records_list)

    if not check:
        log.error("There was an error submitting to Solr!")

    del sources
    del records_to_index
    del records_list
    gc.collect()

    return check
