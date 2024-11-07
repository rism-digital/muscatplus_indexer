import orjson

from indexer.helpers.identifiers import ProjectIdentifiers


def create_institution_index_document(record, cfg: dict) -> list[dict]:
    institution_id: str = f"cantus_institution_{record['id']}"

    d: dict = {
        "id": institution_id,
        "institution_id": institution_id,
        "type": "institution",
        "project_s": ProjectIdentifiers.CANTUS,
        "record_uri_sni": f"https://cantusdatabase.org/institution/{record['id']}",
        "name_s": record["name"],
        "has_siglum_b": False,
        "total_sources_i": 0,
        "city_s": record["city"],
        "related_sources_json": orjson.dumps([]).decode("utf-8"),
        "created": record["created"].strftime("%Y-%m-%dT%H:%M:%SZ"),
        "updated": record["updated"].strftime("%Y-%m-%dT%H:%M:%SZ"),
    }

    return [d]
