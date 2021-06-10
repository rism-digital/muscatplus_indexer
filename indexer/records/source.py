import re
from typing import List, Dict, Optional, TypedDict, Tuple

import pymarc
import ujson
import verovio
import yaml

import logging
from simhash import fingerprint, fnvhash

from indexer.helpers.identifiers import RECORD_TYPES_BY_ID
from indexer.helpers.marc import create_marc
from indexer.helpers.utilities import normalize_id, to_solr_single_required, to_solr_single
from indexer.helpers.profiles import process_marc_profile
from indexer.processors import source as source_processor
from indexer.records.holding import HoldingIndexDocument, holding_index_document


log = logging.getLogger("muscat_indexer")
index_config: Dict = yaml.full_load(open("index_config.yml", "r"))

source_profile: Dict = yaml.full_load(open('profiles/sources.yml', 'r'))
vrv_tk = verovio.toolkit()
verovio.enableLog(False)
vrv_tk.setInputFrom(verovio.PAE)
vrv_tk.setOptions(ujson.dumps({
    "footer": 'none',
    "header": 'none',
    "breaks": 'none',
}))


def create_source_index_documents(record: Dict) -> List:
    source: str = record['marc_source']
    marc_record: pymarc.Record = create_marc(source)

    record_type_id: int = record['record_type']
    # A source is always either its own member, or belonging to group of sources
    # all with the same "parent" source. This is stored in the database in the 'source_id'
    # field as either a NULL value, or the ID of the parent source.
    # If it is NULL then use the source id, indicating that it belongs to a group of 1, itself.
    # If it points to another source, use that.
    # NB: this means that a parent source will have its own ID here, while
    # all the 'children' will have a different ID. This is why the field is not called
    # 'parent_id', since it can gather all members of the group, *including* the parent.
    membership_id: int = m if (m := record.get('source_id')) else record['id']
    rism_id: str = normalize_id(to_solr_single_required(marc_record, '001'))
    source_id: str = f"source_{rism_id}"
    num_holdings: int = record.get("holdings_count")
    main_title: str = record['std_title']

    # This normalizes the holdings information to include manuscripts. This is so when a user
    # wants to see all the sources in a particular institution we can simply filter by the institution
    # id on the sources, regardless of whether they have a holding record, or they are a MS.
    manuscript_holdings: List = _get_manuscript_holdings(marc_record, source_id, main_title) or []
    holding_orgs: List = _get_holding_orgs(manuscript_holdings, record.get("holdings_org"), record.get("parent_holdings_org")) or []
    holding_orgs_ids: List = _get_holding_orgs_ids(manuscript_holdings, record.get("holdings_marc"), record.get("parent_holdings_marc")) or []
    holding_orgs_identifiers: List = _get_full_holding_identifiers(manuscript_holdings, record.get("holdings_marc"), record.get("parent_holdings_marc")) or []

    parent_record_type_id: Optional[int] = record.get("parent_record_type")
    source_membership_json: Optional[Dict] = None
    if parent_record_type_id:
        source_membership_json = {
            "source_id": f"source_{membership_id}",
            "main_title": record.get("parent_title"),
        }

    people_names: List = list({n.strip() for n in d.split("\n") if n}) if (d := record.get("people_names")) else []
    variant_people_names: Optional[List] = _get_variant_people_names(record.get("alt_people_names"))
    related_people_ids: List = list({f"person_{n}" for n in d.split("\n") if n}) if (d := record.get("people_ids")) else []

    # add some core fields to the source. These are fields that may not be easily
    # derived directly from the MARC record, or that include data from the database.
    source_core: Dict = {
        "id": source_id,
        "type": "source",
        "rism_id": rism_id,
        "source_id": source_id,
        "source_membership_id": f"source_{membership_id}",
        "source_membership_title_s": record.get("parent_title"),  # the title of the parent record; can be NULL.
        "source_membership_json": ujson.dumps(source_membership_json) if source_membership_json else None,
        "main_title_s": main_title,  # uses the std_title column in the Muscat database; cannot be NULL.
        "num_holdings_i": 1 if num_holdings == 0 else num_holdings,  # every source has at least one exemplar
        "holding_institutions_sm": holding_orgs,
        "holding_institutions_identifiers_sm": holding_orgs_identifiers,
        "holding_institutions_ids": holding_orgs_ids,
        "subtype_s": RECORD_TYPES_BY_ID.get(record_type_id),
        "people_names_sm": people_names,
        "variant_people_names_sm": variant_people_names,
        "related_people_ids": related_people_ids,
        "is_item_record_b": source_id != f"source_{membership_id}",  # false if this is a parent record; true if a child
        "created": record["created"].strftime("%Y-%m-%dT%H:%M:%SZ"),
        "updated": record["updated"].strftime("%Y-%m-%dT%H:%M:%SZ")
    }

    # Process the MARC record and profile configuration and add additional fields
    additional_fields: Dict = process_marc_profile(source_profile, source_id, marc_record, source_processor)
    source_core.update(additional_fields)

    # Extended incipits have their fingerprints calculated for similarity matching.
    # They are configurable because they slow down indexing considerably, so can be disabled
    # if faster indexing is needed.
    extended_incipits: bool = index_config['indexing']['extended_incipits']
    incipits: List = _get_incipits(marc_record, source_id, main_title, extended_incipits) or []

    res: List = [source_core]
    res.extend(incipits)
    res.extend(manuscript_holdings)

    return res


def _get_manuscript_holdings(record: pymarc.Record, source_id: str, main_title: str) -> Optional[List[HoldingIndexDocument]]:
    """
        Create a holding record for sources that do not actually have a holding record, e.g., manuscripts
        This is so that we can provide a unified interface for searching all holdings of an institution
        using the holding record mechanism, rather than a mixture of several different record types.
    """
    # First check to see if the record has 852 fields; if it doesn't, skip trying to process any further.
    if "852" not in record:
        return None

    source_num: str = to_solr_single_required(record, '001')
    holding_institution_ident: Optional[str] = to_solr_single(record, "852", "x")
    # Since these are for MSS, the holding ID is created by tying together the source id and the institution id; this
    # should result in a unique identifier for this holding record.
    holding_id: str = f"holding_{holding_institution_ident}-{source_id}"
    holding_record_id: str = f"{holding_institution_ident}-{source_num}"

    return [holding_index_document(record, holding_id, holding_record_id, source_id, main_title)]


def _get_variant_people_names(variant_names: Optional[str]) -> Optional[List]:
    """
    There is no need to index all the name variants, only the unique tokens in the
    variant names. This splits the list of variant names into tokens, and then
    adds them to a set, which has the effect of removing duplicate tokens.

    :param variant_names: A string representing a newline-separated list of variant names
    :return: A list of unique name tokens.
    """
    if not variant_names:
        return None

    list_of_names: List = variant_names.split("\n")
    unique_tokens = set()
    for name in list_of_names:
        name_parts: List = [n.strip() for n in re.split(r",| ", name) if n and len(n) > 2]
        unique_tokens.update(name_parts)

    return list(unique_tokens)


def _get_holding_orgs(mss_holdings: List[HoldingIndexDocument], print_holdings: Optional[str] = None, parent_holdings: Optional[str] = None) -> Optional[List[str]]:
    # Coalesces both print and mss holdings into a multivalued field so that we can filter sources by their holding
    # library
    # If there are any holding records for MSS, get the siglum. Use a set to ignore any duplicates
    sigs: set[str] = set()

    for mss in mss_holdings:
        if siglum := mss.get("siglum_s"):
            sigs.add(siglum)

    all_holdings: List = []

    if print_holdings:
        all_holdings += print_holdings.split("\n")

    if parent_holdings:
        all_holdings += parent_holdings.split("\n")

    for lib in all_holdings:
        if siglum := lib.strip():
            sigs.add(siglum)

    return list(sigs)


def _get_holding_orgs_ids(mss_holdings: List[HoldingIndexDocument], print_holdings: Optional[str] = None, parent_holdings: Optional[str] = None) -> List[str]:
    ids: set[str] = set()

    for mss in mss_holdings:
        if inst_id := mss.get("institution_id"):
            ids.add(inst_id)

    all_marc_records: List = []

    if print_holdings:
        all_marc_records += print_holdings.split("\n")

    if parent_holdings:
        all_marc_records += parent_holdings.split("\n")

    for rec in all_marc_records:
        rec = rec.strip()
        m: pymarc.Record = create_marc(rec)

        if inst := to_solr_single(m, "852", "x"):
            ids.add(f"institution_{inst}")

    return list(ids)


def _get_full_holding_identifiers(mss_holdings: List[HoldingIndexDocument], print_holdings: Optional[str] = None, parent_holdings: Optional[str] = None) -> List[str]:
    ids: set[str] = set()

    for mss in mss_holdings:
        institution_sig: str = mss.get("siglum_s", "")
        institution_name: str = mss.get("institution_s", "")
        institution_shelfmark: str = mss.get("shelfmark_s", "")
        ids.add(f"{institution_name} {institution_sig} {institution_shelfmark}")

    all_marc_records: List = []

    if print_holdings:
        all_marc_records += print_holdings.split("\n")
    if parent_holdings:
        all_marc_records += parent_holdings.split("\n")

    for rec in all_marc_records:
        rec = rec.strip()
        m: pymarc.Record = create_marc(rec)

        rec_sig: str = to_solr_single(m, "852", "a") or ""
        rec_shelfmark: str = to_solr_single(m, "852", "c") or ""
        rec_name: str = to_solr_single(m, "852", "e") or ""
        ids.add(f"{rec_name} {rec_sig} {rec_shelfmark}")

    return list(ids)

class IncipitIndexDocument(TypedDict):
    id: str
    type: str
    source_id: str
    incipit_num_i: int
    work_num_s: str
    music_incipit_s: Optional[str]
    text_incipit_s: Optional[str]
    role_s: Optional[str]
    title_s: Optional[str]
    key_mode_s: Optional[str]
    key_s: Optional[str]
    timesig_s: Optional[str]
    clef_s: Optional[str]
    general_notes_sm: Optional[List[str]]
    scoring_sm: Optional[List[str]]


def _incipit_to_pae(incipit: Dict) -> str:
    """
    :param incipit: A Dict result object for an incipit.
    :return: A string formatted as Plaine and Easie code
    """
    pae_code: List = ["@start:pae-file"]

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

    pae_code.append("@end:pae-file")

    return "\n".join(pae_code)


def normalized_pae(pae_code: str) -> str:
    try:
        vrv_tk.loadData(pae_code)
        pae = vrv_tk.renderToPAE()
    except Exception as e:
        pae = f"Error with pae conversion: {e}"

    return pae


UPPERCASE_PITCH_REGEX = re.compile(r"([\dA-Gxbn]+)")
DATA_FIELD_REGEX = re.compile(r"^@data:(.*)$", re.MULTILINE)


def __incipit(field: pymarc.Field, source_id: str, source_title: str, creator_name: Optional[str], num: int, extended_indexing: bool) -> IncipitIndexDocument:
    work_number: str = f"{field['a']}.{field['b']}.{field['c']}"

    d: Dict = {
        "id": f"{source_id}_incipit_{num}",
        "type": "incipit",
        "source_id": source_id,
        "source_title_s": source_title,
        "creator_name_s": creator_name,
        "incipit_num_i": num,
        "music_incipit_s": field['p'],
        "text_incipit_s": field['t'],
        "title_s": field['d'],
        "role_s": field['e'],
        "work_num_s": work_number,
        "key_mode_s": field['r'],
        "key_s": field['n'],
        "timesig_s": field['o'],
        "clef_s": field['g'],
        "general_notes_sm": field.get_subfields('q'),
        "scoring_sm": field.get_subfields('z'),
    }
    pae_code: Optional[str] = _incipit_to_pae(d) if field['p'] else None
    d["original_pae_sni"] = pae_code

    # If extended indexing is enabled, run the PAE through Verovio and calculate
    # the various fingerprints. (This is currently WIP and slows things down quite
    # a bit, so we needed a way to disable it when we're not actively working on
    # incipit searching.)
    if pae_code and extended_indexing:
        d["original_fingerprint_lp"] = fingerprint(list(map(fnvhash, pae_code)))
        norm_pae: str = normalized_pae(pae_code)
        d["normalized_fingerprint_lp"] = fingerprint(list(map(fnvhash, norm_pae)))
        d["normalized_pae_sni"] = norm_pae

    return d


def _get_incipits(record: pymarc.Record, source_id: str, source_title: str, extended_indexing: bool) -> Optional[List]:
    incipits: List = record.get_fields("031")
    if not incipits:
        return None

    creator_name: Optional[str] = to_solr_single(record, "100", "a")

    return [__incipit(f, source_id, source_title, creator_name, num, extended_indexing) for num, f in enumerate(incipits)]
