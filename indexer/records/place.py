import pymarc
import yaml

from indexer.helpers.marc import create_marc
from indexer.helpers.profiles import compile_marc_profile, process_marc_profile
from indexer.helpers.utilities import clean_multivalued
from indexer.processors import place as place_processor

raw_place_profile: dict = yaml.full_load(open("profiles/places.yml"))  # noqa: SIM115
place_profile = compile_marc_profile(raw_place_profile, place_processor)


def create_place_index_document(record: dict, cfg: dict) -> dict:
    """
    Places are not stored as MARC records, so the dictionary that is returned from the
    MySQL query is indexed directly.

    :param place: A dictionary result from the places table
    :return: A Solr index document.
    """
    rism_id: str = record["id"]
    marc_record: pymarc.Record = create_marc(record["marc_source"])
    place_id: str = f"place_{rism_id}"

    core_place: dict = {
        "id": place_id,
        "rism_id": rism_id,
        "full_rism_id": f"places/{rism_id}",
        "type": "place",
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

    additional_fields: dict = process_marc_profile(
        place_profile, place_id, marc_record, dbdata=record
    )
    core_place.update(additional_fields)

    return core_place
