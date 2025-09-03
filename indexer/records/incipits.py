import logging
from collections import defaultdict
from typing import TypedDict

import orjson
import pymarc
import verovio
import yaml

from indexer.helpers.identifiers import get_record_type, get_source_type
from indexer.helpers.utilities import (
    get_content_types,
    get_titles,
)

log = logging.getLogger("muscat_indexer")
index_config: dict = yaml.full_load(open("index_config.yml"))  # noqa: SIM115

verovio.enableLog(False)  # noqa
VEROVIO_OPTIONS = {
    "xmlIdChecksum": True,
}
vrv_tk = verovio.toolkit()
vrv_tk.setInputFrom("pae")
vrv_tk.setOptions(VEROVIO_OPTIONS)


class IncipitIndexDocument(TypedDict):
    id: str
    type: str
    source_id: str
    incipit_num_i: int
    incipit_len_i: int
    work_num_s: str
    music_incipit_s: str | None
    text_incipit_s: str | None
    role_s: str | None
    titles_sm: str | None
    key_mode_s: str | None
    key_s: str | None
    timesig_s: str | None
    clef_s: str | None
    is_mensural_b: bool
    general_notes_sm: list[str] | None
    scoring_sm: list[str] | None


def check_unique_identifiers(
    fields: list[pymarc.Field], document_id: str, check_format: bool = True
) -> bool:
    """Ensures the identifiers combine to create a unique incipit ident."""
    work_numbers = set()
    for incipit in fields:
        work_number = _get_work_number(incipit, document_id, check_format)
        if work_number in work_numbers:
            log.error("Duplicate incipit number: %s for %s", work_number, document_id)
            return False
        work_numbers.add(work_number)
    return True


def fix_unique_identifiers(
    fields: list[pymarc.Field], document_id: str
) -> list[pymarc.Field]:
    work_fields = defaultdict(list)

    fixed_fields = []
    for incipit in fields:
        work_number = _get_work_number(incipit, document_id)
        work_fields[work_number].append(incipit)

    for _, inclist in work_fields.items():
        if len(inclist) == 1:
            fixed_fields.append(inclist[0])
        else:
            fixed_fields.append(inclist[0])
            for f in inclist[1:]:
                c_value = f["c"]
                new_value = f"{c_value}a"
                f["c"] = new_value
                fixed_fields.append(f)

    return fixed_fields


def _incipit_to_pae(incipit: dict) -> str:
    """
    :param incipit: A Dict result object for an incipit.
    :return: A string formatted as Plaine and Easie code
    """
    pae_code: list = []

    if clef := incipit.get("clef_s"):
        pae_code.append(f"@clef:{clef}")
    if timesig := incipit.get("timesig_s"):
        pae_code.append(f"@timesig:{timesig}")
    if key_or_mode := incipit.get("key_mode_s"):
        pae_code.append(f"@key:{key_or_mode}")
    if keysig := incipit.get("key_s"):
        pae_code.append(f"@keysig:{keysig}")
    if incip := incipit.get("music_incipit_s"):
        pae_code.append(f"@data:{incip}")
    if docid := incipit.get("id"):
        pae_code.append(f"@end:{docid}")

    return "\n".join(pae_code)


def _get_pae_features(pae: str) -> dict:
    load_success: bool = vrv_tk.loadData(pae)
    if not load_success:
        log.warning("Verovio could not load PAE %s", pae)
        return {}
    # Verovio is set to render PAE to features
    return vrv_tk.getDescriptiveFeatures({})


def _get_pae_feature_fields(pae_code: str) -> dict:
    d = {"original_pae_sni": pae_code}

    feat: dict = _get_pae_features(pae_code)
    intervals: list = feat.get("intervalsChromatic", [])
    intervals_diat: list = feat.get("intervalsDiatonic", [])
    pitches: list = feat.get("pitchesChromatic", [])
    pitches_diat: list = feat.get("pitchesDiatonic", [])
    interval_ids: list = feat.get("intervalsIds", [])
    pitch_ids: list = feat.get("pitchesIds", [])
    contour_gross: list = feat.get("intervalGrossContour", [])
    contour_refined: list = feat.get("intervalRefinedContour", [])

    # Index the 12 interval fields separately; used for scoring and ranking the document
    # intvfields: dict = _get_intervals(intervals) if intervals else {}
    # d.update(intvfields)

    rend: dict = {
        "intervals_bi": " ".join(intervals) if intervals else None,
        "intervals_diat_bi": " ".join(intervals_diat) if intervals_diat else None,
        "intervals_im": [int(i) for i in intervals] if intervals else None,
        "intervals_diat_im": [int(i) for i in intervals_diat]
        if intervals_diat
        else None,
        "intervals_len_i": len(intervals) if intervals else None,
        "intervals_diat_len_i": len(intervals_diat) if intervals_diat else None,
        "interval_ids_json": orjson.dumps(interval_ids).decode("utf-8")
        if interval_ids
        else None,
        "pitches_bi": " ".join(pitches) if pitches else None,
        "pitches_diat_bi": " ".join(pitches_diat) if pitches_diat else None,
        "pitches_sm": pitches if pitches else None,
        "pitches_diat_sm": pitches_diat if pitches_diat else None,
        "pitches_len_i": len(pitches) if pitches else None,
        "pitches_diat_len_i": len(pitches_diat) if pitches_diat else None,
        "pitches_ids_json": orjson.dumps(pitch_ids).decode("utf-8")
        if pitch_ids
        else None,
        "contour_gross_sm": contour_gross if contour_gross else None,
        "contour_gross_bi": " ".join(contour_gross) if contour_gross else None,
        "contour_refined_sm": contour_refined if contour_refined else None,
        "contour_refined_bi": " ".join(contour_refined) if contour_refined else None,
    }
    d.update(rend)
    # update the record with the verovio features
    return d


def _get_work_number(
    field: pymarc.Field, document_id: str, check_format: bool = True
) -> str:
    work_num = field.get("a", "x")
    mvt_num = field.get("b", "x")
    inc_num = field.get("c", "x")

    if check_format and (
        not work_num.isdigit() or not mvt_num.isdigit() or not inc_num.isdigit()
    ):
        log.error(
            "Incipit numbering is not correct for %s (%s.%s.%s)",
            document_id,
            work_num,
            mvt_num,
            inc_num,
        )

    work_number: str = f"{work_num}.{mvt_num}.{inc_num}"

    if work_number == "x.x.x":
        log.warning("Bad incipit number for %s", document_id)

    return work_number


def _process_incipit_data(field: pymarc.Field, document_id: str) -> dict:
    clef: str | None = field.get("g")
    work_number: str = _get_work_number(field, document_id)

    log.debug("Creating incipits %s %s", document_id, work_number)

    is_mensural: bool = False
    if clef and "+" in clef:
        is_mensural = True

    # This is a rough measure of the length of an incipit is so that we can
    # identify and check the rendering of long incipits.
    music_incipit: str | None = field.get("p")
    incipit_len: int = 0
    if music_incipit:
        # ensure we strip any leading or trailing whitespace.
        music_incipit = music_incipit.strip()
        incipit_len = len(music_incipit)

    # Take the first value if our list of possible time signatures is greater than 0, else take the
    # original field value. This may also be None if field['o'] is None.
    time_signature_data: str | None = field.get("o")

    # if we have more than two space characters in the string, collapse excessive ones into a since space
    # by splitting on space characters and then joining with a single space.
    if isinstance(time_signature_data, str) and time_signature_data.count(" ") > 2:
        log.warning(
            "Excessive spaces in incipit for source %s. Collapsing them.", document_id
        )
        time_signature_data = " ".join(time_signature_data.split())

    tsig_components: list = []
    if time_signature_data and ";" in time_signature_data:
        tsig_components = [
            s.strip() for s in time_signature_data.split(";") if s and s.strip()
        ]

    time_sig: str | None = (
        tsig_components[0] if len(tsig_components) > 0 else time_signature_data
    )

    # If there is a value for the key signature field (and it's not an empty string) then
    # put an 'n' in place so that people can filter for incipits with no key signature.
    key_sig: str = field["n"] if "n" in field and field["n"].strip() else "n"

    norm_key_sig: str = key_sig.replace("[", "").replace("]", "")

    d = {
        "music_incipit_s": music_incipit if incipit_len > 0 else None,
        "has_notation_b": incipit_len > 0,
        "incipit_len_i": incipit_len,
        "text_incipit_sm": field.get_subfields("t"),
        "titles_sm": field.get_subfields("d"),
        "role_s": field.get("e"),
        "key_mode_s": field.get("r"),
        "key_s": key_sig,
        "norm_key_s": norm_key_sig,
        "timesig_s": time_sig.strip() if time_sig and len(time_sig) > 0 else None,
        "clef_s": field.get("g"),
        "voice_instrument_s": field.get("m"),
        "is_mensural_b": is_mensural,
        "general_notes_sm": field.get_subfields("q"),
        "scoring_sm": field.get_subfields("z"),
    }

    return d


def __incipit(
    field: pymarc.Field,
    record_type_id: int,
    parent_record_title: str,
    num: int,
    country_codes: list[str],
    has_digitization: bool,
    record_id,
    record_ident,
    creator,
    source_dates,
    standard_titles,
    is_single_item: bool,
    content_types: list[str],
) -> dict[str, object]:
    work_number = _get_work_number(field, record_ident)

    d: dict = {
        "id": f"{record_ident}_incipit_{work_number}",
        "type": "incipit",
        "parent_type_s": "source",
        "source_id": record_ident,
        "rism_id": record_id,  # index the raw source id to support incipit lookups by source
        "record_type_s": get_record_type(record_type_id, is_single_item),
        "source_type_s": get_source_type(record_type_id),
        "content_types_sm": content_types,
        "work_num_s": work_number,
        # using 'main_title_s' allows us to later serialize this as a source record.
        "main_title_s": parent_record_title,
        "creator_name_s": creator,
        "incipit_num_i": num,
        "country_codes_sm": country_codes,
        "standard_titles_json": orjson.dumps(standard_titles).decode("utf-8")
        if standard_titles
        else None,
        "has_digitization_b": has_digitization,
        "date_ranges_im": source_dates,
    }

    incipit_data: dict = _process_incipit_data(field, record_id)
    d.update(incipit_data)

    pae_code: str | None = _incipit_to_pae(d) if d["music_incipit_s"] else None

    # Run the PAE through Verovio
    if pae_code:
        feats = _get_pae_feature_fields(pae_code)
        d.update(feats)

    return d


def get_source_incipits(
    record: pymarc.Record,
    parent_record_title: str,
    record_type_id: int,
    country_codes: list[str],
    has_digitization: bool,
    creator_name: str | None,
    source_dates: list[int] | None,
) -> list | None:
    if "031" not in record:
        return None

    rism_id: str = record["001"].value()
    source_id: str = f"source_{rism_id}"

    incipits: list = record.get_fields("031")
    all_unique: bool = check_unique_identifiers(incipits, source_id)
    if not all_unique:
        incipits = fix_unique_identifiers(incipits, source_id)

    standard_titles: list[dict] | None = get_titles(record, "240")
    # If a record has neither a 774 (parent -> child) nor a 773 (child -> parent) then it's a single item.
    is_single_item: bool = "774" not in record or "773" not in record
    content_types: list[str] = get_content_types(record)

    return [
        __incipit(
            f,
            record_type_id,
            parent_record_title,
            num,
            country_codes,
            has_digitization,
            rism_id,
            source_id,
            creator_name,
            source_dates,
            standard_titles,
            is_single_item,
            content_types,
        )
        for num, f in enumerate(incipits, 1)
    ]


def __work_incipit(
    field: pymarc.Field,
    num: int,
    work_title: str | None,
    id_num: str,
    document_id: str,
    creator: str | None,
) -> dict:
    work_number: str = _get_work_number(field, document_id)
    d = {
        "id": f"{document_id}_incipit_{work_number}",
        "type": "incipit",
        "parent_type_s": "work",
        "rism_id": id_num,
        "work_id": document_id,
        "work_num_s": work_number,
        "main_title_s": work_title,
        "creator_name_s": creator,
        "incipit_num_i": num,
    }

    incipit_data: dict = _process_incipit_data(field, document_id)
    d.update(incipit_data)

    pae_code: str | None = _incipit_to_pae(d) if d["music_incipit_s"] else None

    # Run the PAE through Verovio
    if pae_code:
        feats = _get_pae_feature_fields(pae_code)
        d.update(feats)

    return d


def get_work_incipits(
    record: pymarc.Record, work_title: str | None, creator_name: str | None
) -> list | None:
    if "031" not in record:
        return None

    rism_id: str = record["001"].value()
    work_id: str = f"work_{rism_id}"

    incipits: list[pymarc.Field] = record.get_fields("031")
    all_unique: bool = check_unique_identifiers(incipits, work_id)
    if not all_unique:
        # Generate a new list of identifiers.
        log.warning("Attempting to automatically fix the incipit identifiers.")
        incipits = fix_unique_identifiers(incipits, work_id)

    return [
        __work_incipit(f, num, work_title, rism_id, work_id, creator_name)
        for num, f in enumerate(incipits, 1)
    ]


def get_inventory_item_incipits(
    record: pymarc.Record,
    source_id: str,
    inventory_item_title: str | None,
    creator_name: str | None,
) -> list | None:
    if "031" not in record:
        return None

    rism_id: str = record["001"].value()
    inventory_item_id: str = f"inventory_item_{rism_id}"
    incipits: list[pymarc.Field] = record.get_fields("031")
    all_unique: bool = check_unique_identifiers(incipits, inventory_item_id)
    if not all_unique:
        incipits = fix_unique_identifiers(incipits, inventory_item_id)

    return [
        __inventory_incipit(
            f,
            source_id,
            num,
            inventory_item_title,
            rism_id,
            inventory_item_id,
            creator_name,
        )
        for num, f in enumerate(incipits, 1)
    ]


def __inventory_incipit(
    field: pymarc.Field,
    source_id: str,
    num: int,
    item_title: str | None,
    id_num: str,
    document_id: str,
    creator: str | None,
) -> dict:
    work_number: str = _get_work_number(field, document_id)

    item_data = {
        "id": f"{document_id}_incipit_{work_number}",
        "type": "incipit",
        "parent_type_s": "inventory_item",
        "rism_id": id_num,
        "source_id": source_id,
        "inventory_item_id": document_id,
        "main_title_s": item_title,
        "creator_name_s": creator,
        "incipit_num_i": num,
    }

    incipit_data: dict = _process_incipit_data(field, document_id)
    item_data.update(incipit_data)

    pae_code: str | None = (
        _incipit_to_pae(item_data) if item_data["music_incipit_s"] else None
    )

    # Run the PAE through Verovio
    if pae_code:
        feats = _get_pae_feature_fields(pae_code)
        item_data.update(feats)

    return item_data
