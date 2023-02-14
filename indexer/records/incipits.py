import logging
from collections import namedtuple
from typing import TypedDict, Optional

import pymarc
import orjson
import verovio
import yaml

from indexer.helpers.datelib import process_date_statements
from indexer.helpers.identifiers import get_record_type, get_source_type
from indexer.helpers.utilities import to_solr_multi, get_creator_name, normalize_id, get_titles, get_content_types

log = logging.getLogger("muscat_indexer")
index_config: dict = yaml.full_load(open("index_config.yml", "r"))

RenderedPAE = namedtuple('RenderedPAE', ['svg', 'midi', 'features'])
verovio.enableLog(False)
VEROVIO_OPTIONS = {
    # "paeFeatures": True,
    "footer": 'none',
    "header": 'none',
    "breaks": 'auto',
    "pageMarginTop": 0,
    "pageMarginBottom": 25,  # Artificially inflate the bottom margin until rism-digital/verovio#1960 is fixed.
    "pageMarginLeft": 0,
    "pageMarginRight": 0,
    # "adjustPageWidth": "true",
    "pageWidth": 2200,
    "spacingStaff": 1,
    "scale": 40,
    "adjustPageHeight": True,
    # "svgHtml5": "true",
    "svgFormatRaw": True,
    "svgRemoveXlink": True,
    "svgViewBox": True,
    "xmlIdChecksum": True
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
    music_incipit_s: Optional[str]
    text_incipit_s: Optional[str]
    role_s: Optional[str]
    titles_sm: Optional[str]
    key_mode_s: Optional[str]
    key_s: Optional[str]
    timesig_s: Optional[str]
    clef_s: Optional[str]
    is_mensural_b: bool
    general_notes_sm: Optional[list[str]]
    scoring_sm: Optional[list[str]]


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
    vrv_tk.loadData(pae)
    # Verovio is set to render PAE to features
    return vrv_tk.getDescriptiveFeatures("{}")


def __incipit(field: pymarc.Field,
              record: pymarc.Record,
              source_id: str,
              record_type_id: int,
              child_type_ids: list[int],
              source_title: str,
              num: int,
              country_codes: list[str]) -> IncipitIndexDocument:
    record_id: str = normalize_id(record['001'].value())
    work_number: str = f"{field['a']}.{field['b']}.{field['c']}"
    clef: Optional[str] = field['g']

    is_mensural: bool = False
    if clef and "+" in clef:
        is_mensural = True

    # This is a rough measure of the length of an incipit is so that we can
    # identify and check the rendering of long incipits.
    music_incipit: Optional[str] = field['p']
    incipit_len: int = 0
    if music_incipit:
        # ensure we strip any leading or trailing whitespace.
        music_incipit = music_incipit.strip()
        incipit_len = len(music_incipit)

    creator: Optional[str] = get_creator_name(record)
    date_statements: Optional[list] = to_solr_multi(record, "260", "c")

    source_dates: list = []
    if date_statements:
        source_dates = process_date_statements(date_statements, record_id)

    # Take the first value if our list of possible time signatures is greater than 0, else take the
    # original field value. This may also be None if field['o'] is None.
    time_signature_data: Optional[str] = field['o']

    # if we have more than two space characters in the string, collapse excessive ones into a since space
    # by splitting on space characters and then joining with a single space.
    if isinstance(time_signature_data, str) and time_signature_data.count(" ") > 2:
        log.warning("Excessive spaces in incipit for source %s. Collapsing them.", record_id)
        time_signature_data = " ".join(time_signature_data.split())

    tsig_components: list = []
    if time_signature_data and ";" in time_signature_data:
        tsig_components = [s.strip() for s in time_signature_data.split(";") if s and s.strip()]

    time_sig: Optional[str] = tsig_components[0] if len(tsig_components) > 0 else time_signature_data

    key_sig: str
    # If there is a value for the key signature field (and it's not an empty string) then
    # put an 'n' in place so that people can filter for incipits with no key signature.
    if field['n'] and field['n'].strip():
        key_sig = field['n']
    else:
        key_sig = "n"

    standard_title_json = get_titles(record, "240")

    d: dict = {
        "id": f"{source_id}_incipit_{num}",
        "type": "incipit",
        "source_id": source_id,
        "rism_id": record_id,  # index the raw source id to support incipit lookups by source
        "record_type_s": get_record_type(record_type_id),
        "source_type_s": get_source_type(record_type_id),
        "content_types_sm": get_content_types(record),
        "main_title_s": source_title,  # using 'main_title_s' allows us to later serialize this as a source record.
        "creator_name_s": creator,
        "incipit_num_i": num,
        "music_incipit_s": music_incipit if incipit_len > 0 else None,
        "has_notation_b": incipit_len > 0,
        "incipit_len_i": incipit_len,
        "text_incipit_s": field['t'],
        "date_ranges_im": source_dates,
        "titles_sm": field.get_subfields("d"),
        "role_s": field['e'],
        "work_num_s": work_number,
        "key_mode_s": field['r'],
        "key_s": key_sig,
        "timesig_s": time_sig.strip() if time_sig and len(time_sig) > 0 else None,
        "clef_s": field['g'],
        "voice_instrument_s": field["m"],
        "is_mensural_b": is_mensural,
        "general_notes_sm": field.get_subfields('q'),
        "scoring_sm": field.get_subfields('z'),
        "country_codes_sm": country_codes,
        "standard_titles_json": orjson.dumps(standard_title_json).decode("utf-8") if standard_title_json else None
    }

    pae_code: Optional[str] = _incipit_to_pae(d) if field['p'] else None

    # Run the PAE through Verovio
    if pae_code:
        d["original_pae_sni"] = pae_code

        feat = _get_pae_features(pae_code)
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
            "intervals_diat_im": [int(i) for i in intervals_diat] if intervals_diat else None,
            "intervals_len_i": len(intervals) if intervals else None,
            "intervals_diat_len_i": len(intervals_diat) if intervals_diat else None,
            "interval_ids_json": orjson.dumps(interval_ids).decode("utf-8") if interval_ids else None,
            "pitches_bi": " ".join(pitches) if pitches else None,
            "pitches_diat_bi": " ".join(pitches_diat) if pitches_diat else None,
            "pitches_sm": pitches if pitches else None,
            "pitches_diat_sm": pitches_diat if pitches_diat else None,
            "pitches_len_i": len(pitches) if pitches else None,
            "pitches_diat_len_i": len(pitches_diat) if pitches_diat else None,
            "pitches_ids_json": orjson.dumps(pitch_ids).decode("utf-8") if pitch_ids else None,
            "contour_gross_sm": contour_gross if contour_gross else None,
            "contour_gross_bi": " ".join(contour_gross) if contour_gross else None,
            "contour_refined_sm": contour_refined if contour_refined else None,
            "contour_refined_bi": " ".join(contour_refined) if contour_refined else None
        }

        # update the record with the verovio features
        d.update(rend)

    return d


def get_incipits(record: pymarc.Record,
                 source_id: str,
                 source_title: str,
                 record_type_id: int,
                 child_type_ids: list[int],
                 country_codes: list[str]) -> Optional[list]:
    if "031" not in record:
        return None

    incipits: list = record.get_fields("031")

    return [__incipit(f, record, source_id, record_type_id, child_type_ids, source_title, num, country_codes) for num, f in enumerate(incipits)]
