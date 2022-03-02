from typing import TypedDict, Optional

from indexer.helpers.utilities import clean_multivalued


class PlaceIndexDocument(TypedDict):
    id: str
    type: str
    name_s: str
    rism_id: str
    country_s: Optional[str]
    district_s: Optional[str]
    alternate_terms_sm: Optional[list[str]]
    topic_sm: Optional[list[str]]
    subtopic_sm: Optional[list[str]]


def create_place_index_document(place: dict, cfg: dict) -> PlaceIndexDocument:
    """
    Places are not stored as MARC records, so the dictionary that is returned from the
    MySQL query is indexed directly.

    :param place: A dictionary result from the places table
    :return: A Solr index document.
    """
    rism_id: str = place.get('id')
    d: PlaceIndexDocument = {
        "id": f"place_{rism_id}",
        "rism_id": rism_id,
        "type": "place",
        "name_s": place["name"],
        "country_s": place.get("country"),
        "district_s": place.get("district"),
        "alternate_terms_sm": clean_multivalued(place, "alternate_terms"),
        "topic_sm": clean_multivalued(place, "topic"),
        "subtopic_sm": clean_multivalued(place, "sub_topic"),
        "sources_count_i": place.get("sources_count", 0),
        "people_count_i": place.get("people_count", 0),
        "institutions_count_i": place.get("institutions_count", 0),
        "holdings_count_i": place.get("holdings_count", 0)
    }

    return d
