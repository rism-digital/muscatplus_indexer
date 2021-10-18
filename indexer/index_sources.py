import logging
from collections import deque
from typing import Generator, List, Dict

from indexer.exceptions import RequiredFieldException
from indexer.helpers.db import mysql_pool
from indexer.helpers.solr import submit_to_solr
from indexer.helpers.utilities import parallelise
from indexer.records.source import create_source_index_documents

log = logging.getLogger("muscat_indexer")


def _get_sources(cfg: Dict) -> Generator[Dict, None, None]:
    log.info("Getting list of sources to index")
    conn = mysql_pool.connection()
    curs = conn.cursor()
    dbname: str = cfg['mysql']['database']

    curs.execute(f"""SELECT child.id AS id, child.title AS title, child.std_title AS std_title,
        child.source_id AS source_id, child.marc_source AS marc_source,
        child.created_at AS created, child.updated_at AS updated,
        child.record_type AS record_type, parent.std_title AS parent_title,
        parent.record_type AS parent_record_type,
        (SELECT COUNT (hh.id) FROM {dbname}.holdings AS hh WHERE hh.source_id = child.id) AS holdings_count,
        (SELECT COUNT(ss.id) FROM {dbname}.sources AS ss WHERE ss.source_id = child.id) as child_count,
        (SELECT GROUP_CONCAT(DISTINCT srt.record_type SEPARATOR ',') FROM {dbname}.sources AS srt WHERE srt.source_id = child.id AND srt.source_id IS NOT NULL GROUP BY srt.source_id) AS child_record_types,
        GROUP_CONCAT(h.marc_source SEPARATOR '\n') AS holdings_marc,
        GROUP_CONCAT(hp.marc_source SEPARATOR '\n') as parent_holdings_marc,
        GROUP_CONCAT(DISTINCT h.lib_siglum SEPARATOR '\n') AS holdings_org,
        GROUP_CONCAT(DISTINCT hp.lib_siglum SEPARATOR '\n') AS parent_holdings_org,
        GROUP_CONCAT(DISTINCT p.full_name SEPARATOR '\n') AS people_names,
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
        WHERE child.wf_stage = 1
        GROUP BY child.id
        ORDER BY child.id desc;""")

    while rows := curs._cursor.fetchmany(cfg['mysql']['resultsize']):  # noqa
        yield rows

    curs.close()
    conn.close()


def index_sources(cfg: Dict) -> bool:
    log.info("Indexing sources")
    source_groups = _get_sources(cfg)
    parallelise(source_groups, index_source_groups)

    return True


def index_source_groups(sources: List) -> bool:
    log.info("Index source group")
    records_to_index: deque = deque()

    for record in sources:
        try:
            docs = create_source_index_documents(record)
        except RequiredFieldException:
            log.critical("Could not index source %s", record["id"])
            continue
        log.debug("Appending source document")
        records_to_index.extend(docs)

    check: bool = submit_to_solr(list(records_to_index))

    if not check:
        log.error("There was an error submitting to Solr!")

    return check
