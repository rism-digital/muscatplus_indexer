from typing import TypedDict, Dict, Optional, List


class PlaceIndexDocument(TypedDict):
    id: str
    type: str
    name_s: str
    country_s: Optional[str]
    district_s: Optional[str]
    alternate_terms_sm: Optional[List[str]]
    topic_sm: Optional[List[str]]
    subtopic_sm: Optional[List[str]]


def create_place_index_document(place: Dict) -> PlaceIndexDocument:
    """
    Places are not stored as MARC records, so the dictionary that is returned from the
    MySQL query is indexed directly.

    :param place: A dictionary result from the places table
    :return: A Solr index document.
    """
    d: PlaceIndexDocument = {
        "id": f"place_{place.get('id')}",
        "type": "place",
        "name_s": place["name"],
        "country_s": place.get("country"),
        "district_s": place.get("district"),
        "alternate_terms_sm": _clean_multivalued(place, "alternate_terms"),
        "topic_sm": _clean_multivalued(place, "topic"),
        "subtopic_sm": _clean_multivalued(place, "sub_topic")
    }

    return d


def _clean_multivalued(place: Dict, field_name: str) -> Optional[List[str]]:
    if not place.get(field_name):
        return None

    return [t for t in place.get(field_name).splitlines() if t.strip()]
