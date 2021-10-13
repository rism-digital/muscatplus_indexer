import logging
from collections import namedtuple
from typing import Dict, TypedDict, Optional, List

import pymarc as pymarc
import ujson
import verovio
import yaml

from indexer.helpers.datelib import process_date_statements
from indexer.helpers.utilities import to_solr_single, to_solr_multi

log = logging.getLogger("muscat_indexer")
index_config: Dict = yaml.full_load(open("index_config.yml", "r"))

RenderedPAE = namedtuple('RenderedPAE', ['svg', 'midi', 'features'])
verovio.enableLog(False)
VEROVIO_OPTIONS = ujson.dumps({
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
})
vrv_tk = verovio.toolkit()
vrv_tk.setInputFrom(verovio.PAE)
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
    title_s: Optional[str]
    key_mode_s: Optional[str]
    key_s: Optional[str]
    timesig_s: Optional[str]
    clef_s: Optional[str]
    is_mensural_b: bool
    general_notes_sm: Optional[List[str]]
    scoring_sm: Optional[List[str]]


def _incipit_to_pae(incipit: Dict) -> str:
    """
    :param incipit: A Dict result object for an incipit.
    :return: A string formatted as Plaine and Easie code
    """
    pae_code: List = []

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


def _render_pae(pae: str) -> dict:
    vrv_tk.loadData(pae)
    # Verovio is set to render PAE to features
    features: str = vrv_tk.getDescriptiveFeatures("{}")
    # svg: str = vrv_tk.renderToSVG()
    # midi: str = vrv_tk.renderToMIDI()
    # b64midi: str = f"data:audio/midi;base64,{midi}"
    feat_output: dict = ujson.loads(features)

    return feat_output


def _get_intervals(intvlist: list) -> dict:
    fields: dict = {}
    for intn, intv in enumerate(intvlist[:12], 1):
        intv_n = int(intv)
        fields[f"interval{intn}_i"] = intv_n

    return fields


def __incipit(field: pymarc.Field, record: pymarc.Record, source_id: str, source_title: str, num: int) -> IncipitIndexDocument:
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

    creator_name: Optional[str] = to_solr_single(record, "100", "a")

    date_statements: Optional[list] = to_solr_multi(record, "260", "c", ungrouped=True)

    source_dates: list = []
    if date_statements:
        source_dates = process_date_statements(record, date_statements)

    d: Dict = {
        "id": f"{source_id}_incipit_{num}",
        "type": "incipit",
        "source_id": source_id,
        "source_title_s": source_title,
        "creator_name_s": creator_name,
        "incipit_num_i": num,
        "music_incipit_s": music_incipit if incipit_len > 0 else None,
        "has_notation_b": incipit_len > 0,
        "incipit_len_i": incipit_len,
        "text_incipit_s": field['t'],
        "date_ranges_im": source_dates,
        "title_s": field['d'],
        "role_s": field['e'],
        "work_num_s": work_number,
        "key_mode_s": field['r'],
        "key_s": field['n'],
        "timesig_s": field['o'],
        "clef_s": field['g'],
        "is_mensural_b": is_mensural,
        "general_notes_sm": field.get_subfields('q'),
        "scoring_sm": field.get_subfields('z'),
    }
    pae_code: Optional[str] = _incipit_to_pae(d) if field['p'] else None
    d["original_pae_sni"] = pae_code

    # If extended indexing is enabled, run the PAE through Verovio
    if pae_code:
        feat = _render_pae(pae_code)
        intervals: Optional[list] = feat.get("intervals")
        pitches: Optional[list] = feat.get("pitches")
        interval_ids: Optional[list] = feat.get("intervalsIds")
        pitch_ids: Optional[list] = feat.get("pitchesIds")

        # Index the 12 interval fields separately; used for scoring and ranking the document
        intvfields: dict = _get_intervals(intervals) if intervals else {}

        d.update(intvfields)

        # Index the incipit features
        d["intervals_mh"] = " ".join(intervals) if intervals else None
        d["intervals_im"] = [int(i) for i in intervals] if intervals else None
        d["interval_ids_json"] = ujson.dumps(interval_ids) if interval_ids else None

        d["pitches_mh"] = " ".join(pitches) if pitches else None
        d["pitches_sm"] = pitches if pitches else None
        d["pitches_ids_json"] = ujson.dumps(pitch_ids) if pitch_ids else None

    return d


def get_incipits(record: pymarc.Record, source_id: str, source_title: str) -> Optional[List]:
    if "031" not in record:
        return None

    incipits: List = record.get_fields("031")

    return [__incipit(f, record, source_id, source_title, num) for num, f in enumerate(incipits)]
