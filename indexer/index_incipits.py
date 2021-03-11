import logging
from typing import Dict, List, Generator

import ujson
import verovio

from indexer.helpers.solr import solr_idx_conn
from indexer.helpers.utilities import parallelise

log = logging.getLogger("muscat_indexer")

tk = verovio.toolkit()
tk.setInputFrom(verovio.pAE)
tk.setOptions(ujson.dumps({
    "footer": 'none',
    "header": 'none'
}))


def _get_incipit_list(cfg: Dict) -> Generator:
    log.debug("Getting incipit list")
    fq: List = ['type:source_incipit', "music_incipit_s:[* TO *]", "work_num_s:1.1.2"]
    fl: List = ["id", "music_incipit_s"]
    res = solr_idx_conn.search("*:*", fq=fq, fl=fl, sort="id asc", rows=1000, cursorMark="*")
    for doc in res:
        yield doc


def index_incipits(cfg: Dict) -> bool:
    log.debug("Indexing incipits")
    incipits_list = _get_incipit_list(cfg)
    parallelise(incipits_list, convert_pae_to_svg)

    solr_idx_conn.commit()
    return True


def convert_pae_to_svg(incipit: Dict) -> bool:
    log.debug(f"Converting pae to svg for {incipit.get('id')}")

    if not incipit.get("music_incipit_s"):
        log.debug("No music incipit!")
        return False

    pae = incipit.get("music_incipit_s")

    tk.loadData(pae)

    svg: str = tk.renderToSVG()

    incipit.update({
        "svg_incipit_unp": svg
    })

    solr_idx_conn.add([incipit], fieldUpdates={"svg_incipit_unp": "add"})

    return True
