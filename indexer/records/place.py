from typing import TypedDict

import pymarc

from indexer.helpers.marc import create_marc
from indexer.helpers.utilities import clean_multivalued


class PlaceIndexDocument(TypedDict):
    id: str
    type: str
    name_s: str
    rism_id: str
    full_rism_id: str
    country_s: str | None
    district_s: str | None
    alternate_terms_sm: list[str] | None
    topic_sm: list[str] | None
    subtopic_sm: list[str] | None
    sources_count_i: int
    people_count_i: int
    institutions_count_i: int
    holdings_count_i: int


def create_place_index_document(record: dict, cfg: dict) -> PlaceIndexDocument:
    """
    Places are not stored as MARC records, so the dictionary that is returned from the
    MySQL query is indexed directly.

    :param place: A dictionary result from the places table
    :return: A Solr index document.
    """
    rism_id: str = record["id"]
    marc_record: pymarc.Record = create_marc(record["marc_source"])

    d: PlaceIndexDocument = {
        "id": f"place_{rism_id}",
        "rism_id": rism_id,
        "full_rism_id": f"places/{rism_id}",
        "type": "place",
        "name_s": record["name"],
        "country_s": record["country"],
        "district_s": record["district"],
        "alternate_terms_sm": clean_multivalued(record, "alternate_terms"),
        "topic_sm": clean_multivalued(record, "topic"),
        "subtopic_sm": clean_multivalued(record, "sub_topic"),
        "sources_count_i": record["sources_count"],
        "people_count_i": record["people_count"],
        "institutions_count_i": record["institutions_count"],
        "holdings_count_i": record["holdings_count"],
    }

    return d
