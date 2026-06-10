import orjson
import pymarc
import yaml

from indexer.helpers.marc import create_marc
from indexer.helpers.profiles import compile_marc_profile, process_marc_profile
from indexer.helpers.utilities import convert_work_catalogue_status, get_person_name
from indexer.processors import publication as publication_processor

raw_publications_profile: dict = yaml.full_load(open("profiles/publications.yml"))  # noqa: SIM115
publications_profile = compile_marc_profile(
    raw_publications_profile, publication_processor
)


def create_publication_index_document(record: dict, cfg: dict) -> dict:
    catalogue: str = record["marc_source"]
    marc_record: pymarc.Record = create_marc(catalogue)
    rism_id: str = marc_record["001"].value()
    publication_id: str = f"publication_{rism_id}"

    work_ids: list = orjson.loads(w) if (w := record["work_ids"]) else []
    composer: str | None = c if (c := record["composer"]) else None
    composer_json: dict | None = orjson.loads(cc) if (cc := record["composer"]) else {}
    composer_name = get_person_name(composer_json) if composer_json else None

    work_catalogue_status: int = record["work_catalogue_status"]

    catalogue_core: dict = {
        "id": publication_id,
        "type": "publication",
        "rism_id": rism_id,
        "full_rism_id": f"publications/{rism_id}",
        "is_work_catalogue_b": True,
        "work_ids": work_ids,
        "works_count_i": len(work_ids),
        "work_catalogue_status_s": convert_work_catalogue_status(work_catalogue_status),
        "composer_json": composer,
        "composer_name_s": composer_name,
        "composer_name_ans": composer_name,
        "created": record["created"].strftime("%Y-%m-%dT%H:%M:%SZ"),
        "updated": record["updated"].strftime("%Y-%m-%dT%H:%M:%SZ"),
    }

    additional_fields: dict = process_marc_profile(
        publications_profile, publication_id, marc_record
    )

    catalogue_core.update(additional_fields)

    return catalogue_core
