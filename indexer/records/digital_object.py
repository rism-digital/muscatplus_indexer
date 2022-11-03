import textwrap

BASE_MUSCAT_URL: str = "https://muscat.rism.info/system/digital_objects/attachments/"


def create_digital_object_index_document(record: dict, cfg: dict) -> dict:
    record_id: int = record["digital_object_id"]
    dobject_id: str = f"dobject_{record_id}"
    linked_record_type: str = record["object_link_type"]
    linked_record_id: int = record["object_link_id"]
    filename: str = record["attachment_file_name"]

    left_padded_id: str = f"{record_id:09}"
    left_padded_path: str = "/".join(textwrap.wrap(left_padded_id, 3))

    # 000/000/014/original/240.jpg

    original_url: str = f"{BASE_MUSCAT_URL}{left_padded_path}/original/{filename}"
    thumb_url: str = f"{BASE_MUSCAT_URL}{left_padded_path}/thumb/{filename}"
    medium_url: str = f"{BASE_MUSCAT_URL}{left_padded_path}/medium/{filename}"

    digital_object: dict = {
        "type": "dobject",
        "id": dobject_id,
        "linked_id": _get_linked_id(linked_record_type, str(linked_record_id)),
        "linked_type_s": linked_record_type.lower(),
        "description_s": record.get("description"),
        "media_type_s": record.get("attachment_content_type"),
        "original_url_s": original_url,
        "thumb_url_s": thumb_url,
        "medium_url_s": medium_url
    }

    return digital_object


def _get_linked_id(obj_type: str, obj_id: str) -> str:
    norm_obj_type: str = obj_type.lower()
    return f"{norm_obj_type}_{obj_id}"
