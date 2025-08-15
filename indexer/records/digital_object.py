import logging
import textwrap

log = logging.getLogger("muscat_indexer")
BASE_MUSCAT_URL: str = "https://muscat.rism.info/system/digital_objects/attachments/"


def create_digital_object_index_document(record: dict, cfg: dict) -> dict:
    record_id: int = record["dobject_id"]
    dobject_id: str = f"dobject_{record_id}"
    linked_record_type: str = record["obj_type"]
    linked_record_id: int = record["obj_id"]
    linked_record_name: str = record["name"]
    filename: str = record["file_name"]
    media_type: str = record["content_type"]

    # NB: URL format example 000/000/014/original/240.jpg
    # Pad the id to 9 chars, split it into groups of 3, and insert "/" between them.
    left_padded_id: str = f"{record_id:09}"
    left_padded_path: str = "/".join(textwrap.wrap(left_padded_id, 3))

    urls: dict = {}
    if media_type in ("image/png", "image/jpeg"):
        urls = {
            "original_url_s": f"{BASE_MUSCAT_URL}{left_padded_path}/original/{filename}",
            "thumb_url_s": f"{BASE_MUSCAT_URL}{left_padded_path}/thumb/{filename}",
            "medium_url_s": f"{BASE_MUSCAT_URL}{left_padded_path}/medium/{filename}",
        }
    elif media_type == "application/xml":
        urls = {
            "encoding_url_s": f"{BASE_MUSCAT_URL}{left_padded_path}/incipits/{filename}"
        }
    else:
        log.warning(
            "Could not determine a media URL for type %s on object %s",
            media_type,
            record_id,
        )

    digital_object: dict = {
        "type": "dobject",
        "id": dobject_id,
        "linked_id": _get_linked_id(linked_record_type, str(linked_record_id)),
        "linked_type_s": linked_record_type.lower(),
        "linked_name_s": linked_record_name,
        "description_s": record.get("description"),
        "media_type_s": record.get("attachment_content_type"),
        **urls,
    }

    return digital_object


def _get_linked_id(obj_type: str, obj_id: str) -> str:
    norm_obj_type: str = obj_type.lower()
    return f"{norm_obj_type}_{obj_id}"
