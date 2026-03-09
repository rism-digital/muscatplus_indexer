import concurrent.futures
import dataclasses
import logging
import re
import timeit
from collections import OrderedDict
from collections.abc import Callable, Iterable
from functools import wraps
from typing import TypedDict

import orjson
import pymarc

from indexer.exceptions import RequiredFieldException
from indexer.helpers.identifiers import (
    WorkPublicationStatusIdentifiers,
    transform_rism_id,
)
from indexer.helpers.marc import create_marc
from indexer.helpers.solr import exists

log = logging.getLogger("muscat_indexer")


def elapsedtime(func) -> Callable:
    """
    Simpler method that just provides the elapsed time for a method call. Used only for the 'main' method
    to provide an elapsed total time for indexing
    :param func:
    :return:
    """

    @wraps(func)
    def timed_f(*args, **kwargs) -> Callable:
        fname = func.__name__
        log.debug(" --- Timing execution for %s ---", fname)
        start = timeit.default_timer()
        ret = func(*args, **kwargs)
        end = timeit.default_timer()
        elapsed: float = end - start

        hours, remainder = divmod(elapsed, 60 * 60)
        minutes, seconds = divmod(remainder, 60)

        log.info(
            "Total time to index %s: %02i:%02i:%02.2f", fname, hours, minutes, seconds
        )
        return ret

    return timed_f


def parallelise(records: Iterable, func: Callable, *args, **kwargs) -> None:
    """
    Given a list of records, this function will parallelise processing of those records. It will
    coalesce the arguments into an array, to be handled by function `func`.

    :param records: A list of records to be processed by `func`. Should be the first argument
    :param func: A function to process and index the records
    :return: None
    """
    with concurrent.futures.ProcessPoolExecutor() as executor:
        futures_list = [
            executor.submit(func, record, *args, **kwargs) for record in records
        ]

        for f in concurrent.futures.as_completed(futures_list):
            f.result()


def to_solr_single(
    record: pymarc.Record,
    field: str,
    subfield: str | None = None,
    ungrouped: bool | None = None,
    sortout: bool | None = True,
) -> str | None:
    """
    Extracts a single value from the MARC record. Always takes the first instance of the
    tag, and the first instance of the subfield within that tag.

    Uses to_solr_multi under the hood; see the comments there to know how this works.
    """
    values: list[str] | None = to_solr_multi(
        record, field, subfield, ungrouped, sortout
    )

    if not values:
        return None

    return values[0]


def to_solr_single_required(
    record: pymarc.Record,
    field: str,
    subfield: str | None = None,
    ungrouped: bool | None = None,
    sortout: bool | None = True,
) -> str:
    """
    Same operations as the to_solr_single, but raises an exception if the value is not found.

    Uses to_solr_multi under the hood; see the comments there to know how this works.
    """
    values: list[str] | None = to_solr_multi(
        record, field, subfield, ungrouped, sortout
    )

    if not values:
        record_id: str = record["001"].value()
        log.error(
            "%s requires a value, but one was not found for %s.", field, record_id
        )
        raise RequiredFieldException(
            f"{field} requires a value, but one was not found for {record_id}."
        )

    return values[0]


def _field_matches_grouping(fl: pymarc.Field, grouped: bool) -> bool:
    has_8: bool = fl.get("8") is not None
    return (
        grouped is None
        or (grouped is True and has_8)
        or (grouped is False and not has_8)
    )


def to_solr_multi(
    record: pymarc.Record | None,
    field: str,
    subfield: str | None = None,
    grouped: bool | None = None,
    sortout: bool | None = True,
) -> list[str] | None:
    """
    Returns all the values for a given field and subfield. Extracting this data from the
    field is done by creating an OrderedDict from the keys, and then casting it back to a list. This removes
    duplicates but keeps the original order.

    :param record: A pymarc.Record instance
    :param field: A string indicating the tag that should be extracted
    :param subfield: An optional subfield. If this is not provided, the full value of the field will be returned
        as a MARC string (e.g., $aFoo$bBar).
    :param grouped: Controls the inclusion / exclusion of fields based on the $8 value. See the note below for more
        details.
    :param sortout: If True then the output will be sorted; if False then it will be in record order.
    :return: A list of strings, or None if there wasn't a subfield that was found that matched the parameters.

    "grouped" is a tri-value binary. "True" means get only those values that have a $8 defined. "False" means
    get only those values that do *not* have a $8 defined. "None" means ignore the $8 altogether and get all values.
    Default is "None"

    """
    if not record or field not in record:
        return None

    fields: list[pymarc.Field] = record.get_fields(field)

    # Fast path: whole-field values
    if subfield is None:
        vv = (f.value() for f in fields if f)
        return list(OrderedDict.fromkeys(vv)) or None

    # Slow path
    values: Iterable[str] = (
        subf.strip()
        for fl in fields
        if subfield in fl and _field_matches_grouping(fl, grouped)
        for subf in fl.get_subfields(subfield)
        if subf
    )

    # We want to remove duplicate values, but need to be careful about ordering.
    if sortout:
        # using a set is simpler, but order is not guaranteed.
        return sorted(set(values))

    # Creating a dictionary guarantees insertion order while de-duplicating values
    return list(dict.fromkeys(values))


def to_solr_multi_required(
    record: pymarc.Record,
    field: str,
    subfield: str | None = None,
    ungrouped: bool | None = None,
    sortout: bool | None = True,
) -> list[str]:
    """
    The same operation as to_solr_multi, except this function must return at least one value otherwise it
    will raise an exception.
    """
    ret: list[str] | None = to_solr_multi(record, field, subfield, ungrouped, sortout)

    if ret is None:
        record_id: str = record["001"].value()
        log.error(
            "%s, %s requires a value, but one was not found for %s",
            field,
            subfield,
            record_id,
        )
        raise RequiredFieldException(
            f"{field}, {subfield} requires a value, but one was not found for {record_id}."
        )

    return ret


def clean_multivalued(fields: dict, field_name: str) -> list[str] | None:
    if field_name not in fields or fields[field_name] is None:
        return None

    return [t.strip() for t in fields.get(field_name, "").splitlines() if t.strip()]


def get_external_resources_data(record: pymarc.Record) -> list | None:
    """
    Fetch the external links defined on the record. Note that this will *not* index the links that are linked to
    material group descriptions -- those are handled in the material group indexing section above.
    :param record: A pymarc record
    :return: A list of external links. This will be serialized to a string for storage in Solr.
    """
    if "856" not in record:
        return None

    resources: list = [
        external_resource_data(f)
        for f in record.get_fields("856")
        if f and ("8" not in f or f["8"] != "01")
    ]

    return resources if resources else None


class ExternalResourceDocument(TypedDict, total=False):
    url: str | None
    note: str | None
    link_type: str | None


def external_resource_data(field: pymarc.Field) -> ExternalResourceDocument | None:
    """
    Takes an 856 field and attempts to format a dictionary containing
    the data. Used for adding external links to various places in the indexed records (source, material groups,
    holdings, etc.)

    Due to a misconfiguration, for people the 'Notes' are held in $y at the time of this writing, so we use both fields
    for the notes. See https://github.com/rism-digital/muscat/issues/1081

    :param field: A pymarc.Field. Will return None if the tag is not 856.
    :return: A dictionary of values matching the fields in the 856
    """
    external_resource: ExternalResourceDocument = {}

    if u := field.get("u"):
        external_resource["url"] = u

    if k := field.get("x"):
        external_resource["link_type"] = k

    if (n := field.get("z")) or (n := field.get("y")):
        external_resource["note"] = n

    return external_resource


class PersonRelationshipIndexDocument(TypedDict):
    id: str
    name: str | None
    type: str
    relationship: str | None
    qualifier: str | None
    date_statement: str | None
    person_id: str | None
    this_id: str | None
    this_type: str | None


def related_person(
    field: pymarc.Field,
    this_id: str | None,
    this_type: str | None,
    relationship_number: int,
) -> dict[str, object]:
    """
    Generate a related person record. The target of the relationship is given in the other_person_id field,
    while the source of the relationship is given in the this_id field. Since Sources, Institutions, and People
    can all be related to other people, this_type gives the type of record that we're pointing from.

    Empty values and keys will be removed from the response.

    :param field: The pymarc field for the relationship
    :param this_id: The ID of the source record for the relationship
    :param this_type: The type of the source record (institution, person). Enables ID lookups based on type
    :param relationship_number: An integer corresponding to the position of this relationship in the list of all
        relationships for this person. This is because two people can be related in two different ways, so this
        lets us give a unique number to each enumerated relationship.
    :return: A Solr record for the person relationship
    """
    name: str | None = field.get("a")
    rel_4: str | None = field.get("4")
    rel: str | None = rel_4 or field.get("i")
    pid: str | None = field.get("0")

    if not name:
        log.error(
            "A name was not found for person %s on %s",
            field.get("0"),
            this_id,
        )

    d: PersonRelationshipIndexDocument = {
        "id": f"{relationship_number}",
        "name": name or "[Unknown name]",
        "type": "person",
        # sources use $4 for relationship info; others use $i. Will ultimately return None if neither are found.
        "relationship": rel,
        "qualifier": field.get("j"),
        "date_statement": field.get("d"),
        "person_id": f"person_{pid}" if pid else None,
        "this_id": this_id,
        "this_type": this_type,
    }

    # The main entry (100) field does not have a relator code.
    # If this_id is not set, don't warn about a relator code.
    if this_id and field.tag != "100" and not d.get("relationship"):
        log.warning(
            "A person was saved without a relator code. %s %s", this_id, d.get("name")
        )

    return {k: v for k, v in d.items() if v}


def get_related_people(
    record: pymarc.Record,
    record_id: str | None,
    record_type: str | None,
    fields: tuple[str, ...] = ("500", "700"),
    ungrouped: bool = False,
) -> list[dict[str, object]] | None:
    """
    In some cases you will want to restrict the fields that are used for this lookup. By default it will look at 500
    and 700 fields, since that is where they are kept in the authority records; however, source records use 500 for
    notes. So for sources (and other types, if needed) we can pass in a custom set of fields to look for people
    relationships.

    :param record: a PyMarc record
    :param record_id: The ID of the parent record
    :param record_type: The type of the parent record
    :param fields: An optional Tuple of fields corresponding to the MARC fields where we want to gather this data from.
        Defaults to ("500", "700").
    :param ungrouped: If this is True, this function will only return fields that do not have a $8 value. The default is
        False, indicating all fields, regardless of whether they are grouped or not, will be returned.

    :return: A list of person relationships, or None if not applicable.
    """
    people: list[pymarc.Field] = record.get_fields(*fields)
    if not people:
        return None

    # NB: enumeration starts at 1
    return [
        related_person(p, record_id, record_type, i)
        for i, p in enumerate(people, 1)
        if p and (not ungrouped or "8" not in p)
    ]


class PlaceRelationshipIndexDocument(TypedDict):
    id: str
    name: str | None
    type: str
    relationship: str | None
    place_id: str
    this_id: str
    this_type: str


def __related_place(
    field: pymarc.Field, this_id: str, this_type: str, relationship_number: int
) -> dict[str, object]:
    d: PlaceRelationshipIndexDocument = {
        "id": f"{relationship_number}",
        "type": "place",
        "this_id": this_id,
        "this_type": this_type,
        "name": field.get("a"),
        "relationship": field.get("i", "xp"),
        "place_id": f"place_{field['0']}",
    }

    # strip any null values from the response so that we can do simple checks for available data by looking for the key.
    return {k: v for k, v in d.items() if v}


def get_related_places(
    record: pymarc.Record,
    record_id: str,
    record_type: str,
    fields: tuple[str, ...] = ("551", "751"),
) -> list[dict[str, object]] | None:
    places: list[pymarc.Field] = record.get_fields(*fields)
    if not places:
        return None

    return [
        __related_place(p, record_id, record_type, i)
        for i, p in enumerate(places, 1)
        if p and "0" in p
    ]


class InstitutionRelationshipIndexDocument(TypedDict):
    id: str
    this_id: str
    this_type: str
    name: str | None
    type: str
    place: str | None
    department: str | None
    institution_id: str | None
    relationship: str | None
    qualifier: str | None


def related_institution(
    field: pymarc.Field, this_id: str, this_type: str, relationship_number: int
) -> dict[str, object]:
    relationship_code: str
    if "4" in field:
        relationship_code = field["4"]
    elif "i" in field:
        relationship_code = field["i"]
    else:
        relationship_code = "xi"

    if "a" not in field:
        log.error(
            "A name was not found for institution %s on %s", field.get("0"), this_id
        )

    d: InstitutionRelationshipIndexDocument = {
        "id": f"{relationship_number}",
        "type": "institution",
        "this_id": this_id,
        "this_type": this_type,
        "name": field.get("a", "[Unknown name]"),
        "place": field.get("c"),
        "department": field.get("d"),
        "institution_id": f"institution_{field['0']}",
        "relationship": relationship_code,
        "qualifier": field.get("g"),
    }

    if not d.get("relationship"):
        log.warning(
            "An institution was saved without a relator code. %s %s",
            this_id,
            d.get("name"),
        )

    return {k: v for k, v in d.items() if v}


def get_related_institutions(
    record: pymarc.Record,
    record_id: str,
    record_type: str,
    fields: tuple[str, ...] = ("510", "710"),
    ungrouped: bool = False,
) -> list[dict[str, object]] | None:
    # Due to inconsistencies in authority records, these relationships are held in both 510 and 710 fields.
    institutions: list = record.get_fields(*fields)
    if not institutions:
        return None

    return [
        related_institution(p, record_id, record_type, i)
        for i, p in enumerate(institutions, 1)
        if p and p.get("0") and (not ungrouped or "8" not in p)
    ]


URL_MATCH: re.Pattern = re.compile(
    r"((https?):((//)|(\\\\))+[\w:#@%/;$()~_?+-=\\.&]*)", re.MULTILINE | re.UNICODE
)
OPAC_LINK: re.Pattern = re.compile(
    r"https?://opac\.rism\.info/search\?id=(\d+)&View=rism", re.MULTILINE | re.UNICODE
)
MUSCAT_LINK: re.Pattern = re.compile(
    r"https?://muscat\.rism\.info/admin/sources/(\d+)", re.MULTILINE | re.UNICODE
)


def note_links(note: str) -> str:
    """
    Creates links in notes text. Returns the note with an anchor tag around any plain links.

    Skips adding an anchor if there is already one anchor tag.
    If 'http' is not in the string, will return the note directly.

    :param note: The raw MARC string
    :return: A formatted string.
    """
    # If there are no URLs in this note, don't process any further.
    if "http" not in note:
        return note

    # If the note already contains a single anchor tag, assume that all links are anchored and skip them. This
    # avoids double-encoding anchor tags.
    if "<a href" not in note:
        # Check to see if it's an OPAC or a MUSCAT link; if so, rewrite to an internal link.
        if re.search(OPAC_LINK, note):
            note = OPAC_LINK.sub(
                r'<a href="/sources/\1" _target="blank">RISM Source ID \1</a>', note
            )
        elif re.search(MUSCAT_LINK, note):
            note = MUSCAT_LINK.sub(
                r'<a href="/sources/\1" _target="blank">RISM Source ID \1</a>', note
            )
        else:
            # Any other URLs are passed through wrapped in an anchor tag.
            note = URL_MATCH.sub(r'<a href="\1" _target="blank">\1</a>', note)

    return note


def get_catalogue_numbers(
    field: pymarc.Field, catalogue_fields: list[pymarc.Field] | None
) -> list:
    catalogue_numbers: list = []

    if field.tag == "730" and "n" in field:
        catalogue_numbers.append(field["n"])
    elif field.tag == "240" and catalogue_fields:
        for cfield in catalogue_fields:
            if cfield.tag == "383" and "a" in cfield:
                catalogue_numbers.append(cfield.get("b"))
            elif cfield.tag == "690":
                wv: str = cfield.get("a", "")
                wvno: str = cfield.get("n", "")
                wvtitle: str = f"{wv} {wvno}"
                catalogue_numbers.append(wvtitle.strip())

    return catalogue_numbers


def __title(
    field: pymarc.Field,
    catalogue_fields: list[pymarc.Field] | None,
    holding: pymarc.Field | None,
    source_type: pymarc.Field | None,
) -> dict:
    catalogue_numbers = get_catalogue_numbers(field, catalogue_fields)

    d = {
        "title": field.get("a"),
        "subheading": field.get("k"),
        "arrangement": field.get("o"),
        "key_mode": field.get("r"),
        "catalogue_numbers": catalogue_numbers,
    }

    scoring_summary_f: str | None = field.get("m")
    if scoring_summary_f:
        d["scoring_summary"] = list(
            {val.strip() for val in scoring_summary_f.split(",") if val and val.strip()}
        )

    if holding:
        siglum = holding.get("a")
        shelfmark = holding.get("c")

        d.update({"holding_siglum": siglum, "holding_shelfmark": shelfmark})

    if source_type:
        d.update({"source_type": source_type.get("a")})

    return {k: v for k, v in d.items() if v}


def get_titles(record: pymarc.Record, field: str) -> list[dict] | None:
    """
    Standardize the title field structure. This is used for both the 240 and 730 fields
    since they have similar structure.
    :param record: A pymarc Record
    :param field: The MARC tag; should either be 240 or 730.
    :return: A list of title structures suitable for storing as a JSON field.
    """
    if field not in record:
        return None

    titles = record.get_fields(field)

    c: list[pymarc.Field] | None = None
    h: pymarc.Field | None = None
    y: pymarc.Field | None = None
    if field == "240":
        c = record.get_fields("383", "690")
        if "852" in record:
            h = record.get("852")

        if "593" in record:
            # If the record has a 593 and that is for material group 01, then
            # prefer that for generating the titles. If it does not,
            # then simply take the first 593.
            y = next(
                (f for f in record.get_fields("593") if f.get("8") == "01"),
                record.get("593"),
            )

    return [__title(t, c, h, y) for t in titles if t]


def tokenize_variants(variants: list[str]) -> list[str]:
    """
    If we're only searching, there is no need to index all the term variants, only the unique tokens in the
    variant names. This splits the list of variants into tokens, and then
    adds them to a set, which has the effect of removing any duplicate tokens.

    In other words, if you have the following:

    Bach, Johann Sebastian
    Bach, J Sebastian
    Bach, JS
    Beck, J

    The result will be: [Bach, Johann, Sebastian, Beck, J, JS]

    :param variants: A string representing a newline-separated list of variant terms
    :return: A list of unique name tokens.
    """
    unique_tokens: set = set()

    for variant in variants:
        name_parts: list = [
            n.strip() for n in re.split(r"[, ]", variant) if n and n != "..."
        ]
        unique_tokens.update(name_parts)

    return list(unique_tokens)


def get_creator_name(record: pymarc.Record, suppress_dates: bool = False) -> str | None:
    creator_field: pymarc.Field | None = record.get("100")
    if not creator_field:
        return None

    d = {
        "name": creator_field.get("a", "").strip(),
        "life_dates": creator_field.get("d"),
    }
    return get_person_name(d, suppress_dates)


def get_creator_data(
    record: pymarc.Record,
    record_type: str = "source",
    creator_relationship: str = "cre",
) -> list | None:
    if "100" not in record:
        return None

    record_id: str = record["001"].value()
    solr_record_id: str = f"{record_type}_{record_id}"
    creator = get_related_people(record, solr_record_id, record_type, fields=("100",))
    if not creator:
        return None

    creator[0]["relationship"] = creator_relationship
    return creator


def get_people_names(
    names: list[dict] | None, suppress_dates: bool = False
) -> list[str] | None:
    if not names:
        return None

    out_l: list = []
    for it in names:
        out_l.append(get_person_name(it))

    return out_l


def get_person_name(name: dict, suppress_dates: bool = False) -> str:
    nm: str = name.get("name", "")
    dt = f" ({d})" if not suppress_dates and (d := name.get("life_dates")) else ""
    return f"{nm}{dt}"


@dataclasses.dataclass
class ContentTypes:
    NOTATED_MUSIC = "Notated music"
    LIBRETTO = "Libretto"
    TREATISE = "Treatise"
    MIXED = "Mixed"
    OTHER = "Other"
    INVENTORY = "Inventory"


def get_content_types(record: pymarc.Record | None) -> list[str]:
    """
    Takes all record types associated with this record, and returns a list of
    all possible content types for it.

    Checks if two sets have an intersection set (that they have members overlapping).

    :param record: A pymarc Record field
    :return: A list of index values containing the content types.
    """
    if record is None:
        return []

    all_content_types: list[str] | None = to_solr_multi(record, "593", "b")
    ret: list = []

    if not all_content_types:
        return []

    all_types: set = set(all_content_types)
    if all_types & {ContentTypes.LIBRETTO}:
        ret.append("libretto")

    if all_types & {ContentTypes.TREATISE}:
        ret.append("treatise")

    if all_types & {ContentTypes.NOTATED_MUSIC}:
        ret.append("musical")

    if all_types & {ContentTypes.MIXED}:
        ret.append("mixed")

    if all_types & {ContentTypes.OTHER}:
        ret.append("other")

    if all_types & {ContentTypes.INVENTORY}:
        ret.append("inventory")

    return ret


def get_parent_order_for_members(
    parent_record: pymarc.Record | None, this_id: str
) -> int | None:
    """
    Returns an integer representing the order number of this source with respect to the order of the
    child sources listed in the parent. 0-based, since we simply look up the values in a list.

    If a child ID is not found in a parent record, or if the parent record is None, returns None.

    The form of ID being searched is normalized, so any leading zeros are stripped, etc.

    :param parent_record: The parent record containing the order of the child sources
    :param this_id: The ID of the child to look for in the list. This should have a "source_" or "holding_" prefix.
    :return: An order number as an int, or None if it was not found.
    """
    if not parent_record:
        return None

    if "774" not in parent_record:
        return None

    child_record_fields: list[pymarc.Field] = parent_record.get_fields("774")
    idxs: list = []
    for field in child_record_fields:
        if "w" not in field:
            continue

        subf: list = field.get_subfields("w")
        subf_id = subf[0]
        if not subf_id:
            log.warning(
                f"Problem when searching the membership of {this_id} in {parent_record['001'].value()}."
            )
            continue

        pfx: str = "source_"
        if "4" in field and field["4"] == "holding":
            pfx = "holding_"

        idxs.append(f"{pfx}{subf_id}")

    if this_id in idxs:
        return idxs.index(this_id)

    return None


def update_rism_document(
    record,
    project: str,
    record_type: str,
    label: str,
    cfg: dict,
    additional_fields: dict | None = None,
) -> dict | None:
    document_id: str | None = transform_rism_id(record.get("rism_id"))
    if not document_id:
        return None

    if not exists(document_id, cfg):
        log.error(
            "%s %s does not exist in RISM (%s ID: %s)",
            record_type,
            document_id,
            project,
            record["id"],
        )
        return None

    project_id = record["id"]
    entry: dict = {
        "id": f"{project_id}",
        "type": f"{record_type}",
        "project_type": f"{record.get('project_type')}",
        "project": f"{project}",
        "label": f"{label}",
    }

    if additional_fields:
        entry.update(additional_fields)

    entry_s: str = orjson.dumps(entry).decode("utf-8")

    update_document: dict = {
        "id": document_id,
        "has_external_record_b": {"set": True},
        "external_records_jsonm": {"add-distinct": entry_s},
    }

    if "source_count" in record and record.get("source_count", 0) > 0:
        amount: int = record["source_count"]
        update_document.update(
            {"source_count_i": {"inc": amount}, "total_sources_i": {"inc": amount}}
        )
    return update_document


def get_work_node(
    record: pymarc.Record, record_id: str, record_type: str, source_count: int = 0
) -> dict | None:
    wnid = record["001"].value()
    work_node_id: str = f"work_node_{wnid}"

    if "024" not in record:
        log.warning("Work Node without an 024. Skipping: %s", work_node_id)
        return None

    link_field: pymarc.Field = record["024"]

    if link_field and "2" in link_field and "a" in link_field:
        ident: str = f"{link_field['2'].lower()}:{link_field['a']}"
    else:
        log.warning(
            "Work Node with 024 but without $2 or $a. Skipping: %s", work_node_id
        )
        return None

    creator: pymarc.Field | None = record.get("100")
    if not creator:
        return None

    composer_name: str | None = None
    composer_id: str | None = None
    work_title: str | None = None

    if "a" in creator:
        composer_name: str | None = get_creator_name(record)
        composer_id = f"person_{creator['0']}"

        work_title_subf: str = creator["t"]
        partial_title_subf: str = f". {pt}" if (pt := creator.get("p")) else ""

        work_title = f"{work_title_subf}{partial_title_subf}"

    d: dict = {
        "id": work_node_id,
        "type": "work_node",
        "external_id": ident,
        "composer_name": composer_name,
        "composer_id": composer_id,
        "work_title": work_title,
        "this_id": record_id,
        "this_type": record_type,
        "source_count": source_count if source_count > 0 else None,
    }

    return {k: v for k, v in d.items() if v}


def get_standard_work_titles_data(record: pymarc.Record) -> list[dict] | None:
    if "130" not in record:
        return None

    title: pymarc.Field = record["130"]
    d = {
        "title": title.get("a"),
        "key_mode": title.get("r"),
        "scoring_summary": title.get("m"),
    }
    # Add information from the 690
    catalogue: pymarc.Field = record["690"]
    addn = {"catalogue": catalogue["a"], "number_page": catalogue["n"]}
    d.update(addn)

    # "standard_titles_data" is typically a list, so we return a list even if it just has a single entry.
    return [{k: v for k, v in d.items() if v}]


def get_work_record(
    record: pymarc.Record, record_id: str, record_type: str
) -> dict | None:
    work_id = record["001"].value()
    d = {
        "id": work_id,
        "type": "work",
        "this_id": record_id,
        "this_type": record_type,
    }

    titles: list | None = get_standard_work_titles_data(record)
    if titles:
        d.update(titles[0])

    catalogue_entry = record["690"]
    d["catalogue"] = catalogue_entry["a"]
    d["number_page"] = catalogue_entry["n"]

    return d


def get_related_sources(
    related: list, relationship_fields: list[pymarc.Field], host_source_id: str
) -> list[dict] | None:
    """
    Combines the MARC source from related sources and the 787 entries from a record to create a JSON
    field for the related sources.

    :param related: A string containing the record IDs and MARC entries, delimited by "|~|" between the related sources
        and by "|:|" between the ID and MARC.
    :param relationship_fields: A list of 787 fields from the source MARC. Needed because this is the only place any
        notes about the relationship are stored.
    :param host_source_id: The id of the parent source record.
    :return: A list of related sources in JSON format.
    """
    # =787  0#$nT p: Solo and Chorus ... From Cantata of
    # "Daniel" ... Copied from the Sabbath Bell by / S[amuel] F[rederick] Van Vleck.
    # organist / Nov. 22 1878.$w1001125501$4rdau:P60311
    notes: dict = {}

    for relfield in relationship_fields:
        sid = relfield.get("w")
        snote = relfield.get("n")
        if sid and snote:
            notes[sid] = snote

    related_entries: list = []
    for relationship_id, individual_record in enumerate(related, 1):
        relator_code = individual_record["relator_code"]
        relmarc_source = individual_record["marc_source"]

        rel_marc_record: pymarc.Record | None = (
            create_marc(relmarc_source) if relmarc_source else None
        )

        if not rel_marc_record:
            log.error("Could not load foreign MARC record")
            continue

        record_id = rel_marc_record["001"].value()

        source_id: str = f"source_{record_id}"
        title: list[dict[str, object]] | None = get_titles(rel_marc_record, "240")

        note: str | None = None
        if record_id in notes:
            note = notes[record_id]

        d = {
            "id": f"{relationship_id}",
            "type": "source",
            "source_id": source_id,
            "relationship": relator_code,
            "title": title,
            "note": note,
            "this_id": host_source_id,
            "this_type": "source",
        }

        related_entries.append({k: v for k, v in d.items() if v})

    return related_entries


def convert_work_catalogue_status(work_catalogue_status: int) -> str:
    match work_catalogue_status:
        case WorkPublicationStatusIdentifiers.COMPLETED:
            return "completed"
        case WorkPublicationStatusIdentifiers.ALTERNATE:
            return "alternate"
        case WorkPublicationStatusIdentifiers.PARTIALLY_COMPLETED:
            return "partial"
        case WorkPublicationStatusIdentifiers.ELIGIBLE:
            return "eligible"
        case _:
            # This should not happen, but just in case...
            return "not-a-work-catalogue"
