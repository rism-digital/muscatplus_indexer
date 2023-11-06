from typing import Optional

from diamm_indexer.helpers.identifiers import RELATOR_MAP
from indexer.helpers.identifiers import ProjectIdentifiers


def get_related_sources_json(sources: Optional[str]) -> list[dict]:
    if not sources:
        return []

    sources_raw: list[str] = sources.split("\n")

    sources_json: list = []
    for source in sources_raw:
        siglum, shelfmark, name, relnum, uncertain, source_id = source.split("||")
        title = name if name else "[No title]"
        relator_code = RELATOR_MAP.get(relnum, "unk")

        d = {
            "id": f"diamm_source_{source_id}",
            "type": "source",
            "project": ProjectIdentifiers.DIAMM,
            "project_type": "sources",
            "source_id": f"diamm_source_{source_id}",
            "title": [{"title": title,
                       "source_type": "Manuscript copy",
                       "holding_shelfmark": shelfmark,
                       "holding_siglum": siglum}],
            "relationship": relator_code,
            "qualifier": "Doubtful" if uncertain == "t" else "Ascertained"
        }

        sources_json.append(d)

    return sources_json