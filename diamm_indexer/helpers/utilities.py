from diamm_indexer.helpers.identifiers import RELATOR_MAP
from indexer.helpers.identifiers import ProjectIdentifiers


def get_related_sources_json(sources: list[dict] | None) -> list[dict]:
    if not sources:
        return []

    sources_json: list = []
    for source in sources:
        title = source["name"] if source["name"] else "[No title]"
        relnum = str(source["relationship_type_id"])
        relator_code = RELATOR_MAP.get(relnum, "unk")
        source_id = f"diamm_source_{source['id']}"

        d = {
            "id": source_id,
            "type": "source",
            "project": ProjectIdentifiers.DIAMM,
            "project_type": "sources",
            "source_id": source_id,
            "title": [
                {
                    "title": title,
                    "source_type": "Manuscript copy",
                    "holding_shelfmark": source["shelfmark"],
                    "holding_siglum": source["siglum"],
                }
            ],
            "relationship": relator_code,
            "qualifier": (
                "Alleged" if source["relationship_uncertain"] else "Ascertained"
            ),
            "note": source["relationship_type_name"],
        }

        sources_json.append(d)

    return sources_json
