import concurrent.futures
import dataclasses
import logging
import re
import timeit
from collections import OrderedDict
from functools import wraps
from typing import Callable, Iterable, Optional, Pattern, TypedDict

import orjson
import pymarc

from indexer.exceptions import MalformedIdentifierException, RequiredFieldException
from indexer.helpers.identifiers import transform_rism_id
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
    :param func: A shared Solr connection object
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
    subfield: Optional[str] = None,
    ungrouped: Optional[bool] = None,
    sortout: Optional[bool] = True,
) -> Optional[str]:
    """
    Extracts a single value from the MARC record. Always takes the first instance of the
    tag, and the first instance of the subfield within that tag.

    Uses to_solr_multi under the hood; see the comments there to know how this works.
    """
    values: Optional[list[str]] = to_solr_multi(
        record, field, subfield, ungrouped, sortout
    )

    if not values:
        return None

    return values[0]


def to_solr_single_required(
    record: pymarc.Record,
    field: str,
    subfield: Optional[str] = None,
    ungrouped: Optional[bool] = None,
    sortout: Optional[bool] = True,
) -> str:
    """
    Same operations as the to_solr_single, but raises an exception if the value is not found.

    Uses to_solr_multi under the hood; see the comments there to know how this works.
    """
    values: Optional[list[str]] = to_solr_multi(
        record, field, subfield, ungrouped, sortout
    )

    if not values:
        record_id: str = normalize_id(record["001"].value())
        log.error(
            "%s requires a value, but one was not found for %s.", field, record_id
        )
        raise RequiredFieldException(
            f"{field} requires a value, but one was not found for {record_id}."
        )

    return values[0]


def to_solr_multi(
    record: Optional[pymarc.Record],
    field: str,
    subfield: Optional[str] = None,
    grouped: Optional[bool] = None,
    sortout: Optional[bool] = True,
) -> Optional[list[str]]:
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

    if subfield is None:
        return list(OrderedDict.fromkeys(f.value() for f in fields if f))

    # Treat the subfields as a list of lists, and flatten their values. `get_subfields` returns a list,
    # and we are dealing with a list of fields, so we iterate twice here: Once over the fields, and then
    # over the values in each field.
    retval: list[str] = []

    for fl in fields:
        if subfield not in fl:
            continue

        if (
            grouped is True
            and fl.get("8") is not None
            or grouped is False
            and fl.get("8") is None
            or grouped is None
        ):
            retval += [subf.strip() for subf in fl.get_subfields(subfield)]
        else:
            # Skip anything else and don't do anything.
            continue

    if not retval:
        return None

    # We want to remove duplicate values, but need to be careful about ordering.
    if sortout:
        # using a set is simpler, but order is not guaranteed.
        return sorted(set(retval))

    # Creating a dictionary guarantees insertion order.
    return list(dict.fromkeys(retval))


def to_solr_multi_required(
    record: pymarc.Record,
    field: str,
    subfield: Optional[str] = None,
    ungrouped: Optional[bool] = None,
    sortout: Optional[bool] = True,
) -> list[str]:
    """
    The same operation as to_solr_multi, except this function must return at least one value otherwise it
    will raise an exception.
    """
    ret: Optional[list[str]] = to_solr_multi(
        record, field, subfield, ungrouped, sortout
    )

    if ret is None:
        record_id: str = normalize_id(record["001"].value())
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


def normalize_id(identifier: str) -> str:
    """
    Muscat IDs come in a wide variety of shapes and sizes, some with leading zeroes, others without.

    This method ensures any identifier is consistent by stripping any leading zeroes off a string. This is done
    by parsing it as an integer, and then returning it as a string again.

    :param identifier: An identifier to normalize
    :return: A normalized identifier
    """

    try:
        idval: int = int(identifier)
    except ValueError as err:
        raise MalformedIdentifierException(
            f"The identifier {identifier} is not well-formed."
        ) from err

    return f"{idval}"


def clean_multivalued(fields: dict, field_name: str) -> Optional[list[str]]:
    if field_name not in fields or fields[field_name] is None:
        return None

    return [t.strip() for t in fields.get(field_name, "").splitlines() if t.strip()]


class ExternalResourceDocument(TypedDict, total=False):
    url: Optional[str]
    note: Optional[str]
    link_type: Optional[str]


def external_resource_data(field: pymarc.Field) -> Optional[ExternalResourceDocument]:
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
    name: Optional[str]
    type: str
    relationship: Optional[str]
    qualifier: Optional[str]
    date_statement: Optional[str]
    person_id: str
    this_id: str
    this_type: str


def related_person(
    field: pymarc.Field, this_id: str, this_type: str, relationship_number: int
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
    if "a" not in field:
        log.error("A name was not found for person %s on %s", field.get("0"), this_id)

    d: PersonRelationshipIndexDocument = {
        "id": f"{relationship_number}",
        "name": field.get("a", "[Unknown name]"),
        "type": "person",
        # sources use $4 for relationship info; others use $i. Will ultimately return None if neither are found.
        "relationship": field.get("4") if "4" in field else field.get("i"),
        "qualifier": field.get("j"),
        "date_statement": field.get("d"),
        "person_id": f"person_{field.get('0')}",
        "this_id": this_id,
        "this_type": this_type,
    }

    # The main entry (100) field does not have a relator code.
    if not d.get("relationship") and field.tag != "100":
        log.warning(
            "A person was saved without a relator code. %s %s", this_id, d.get("name")
        )

    return {k: v for k, v in d.items() if v}


def get_related_people(
    record: pymarc.Record,
    record_id: str,
    record_type: str,
    fields: tuple = ("500", "700"),
    ungrouped: bool = False,
) -> Optional[list[dict[str, object]]]:
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
    record_tags: set = {f.tag for f in record}
    if set(fields).isdisjoint(record_tags):
        return None

    people: list[pymarc.Field] = record.get_fields(*fields)

    # NB: enumeration starts at 1
    if ungrouped:
        return [
            related_person(p, record_id, record_type, i)
            for i, p in enumerate(people, 1)
            if p and "8" not in p
        ]
    return [
        related_person(p, record_id, record_type, i)
        for i, p in enumerate(people, 1)
        if p
    ]


class PlaceRelationshipIndexDocument(TypedDict):
    id: str
    name: Optional[str]
    type: str
    relationship: Optional[str]
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
    fields: tuple = ("551", "751"),
) -> Optional[list[dict[str, object]]]:
    record_tags: set = {f.tag for f in record}
    if set(fields).isdisjoint(record_tags):
        return None
    places: list[pymarc.Field] = record.get_fields(*fields)

    return [
        __related_place(p, record_id, record_type, i)
        for i, p in enumerate(places, 1)
        if p and "0" in p
    ]


class InstitutionRelationshipIndexDocument(TypedDict):
    id: str
    this_id: str
    this_type: str
    name: Optional[str]
    type: str
    place: Optional[str]
    department: Optional[str]
    institution_id: Optional[str]
    relationship: Optional[str]
    qualifier: Optional[str]


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
    fields: tuple = ("510", "710"),
    ungrouped: bool = False,
) -> list[dict[str, object]] | None:
    # Due to inconsistencies in authority records, these relationships are held in both 510 and 710 fields.
    record_tags: set = {f.tag for f in record}
    if set(fields).isdisjoint(record_tags):
        return None

    institutions: list = record.get_fields(*fields)

    if ungrouped:
        return [
            related_institution(p, record_id, record_type, i)
            for i, p in enumerate(institutions, 1)
            if p and p.get("0") and ("8" not in p)
        ]
    return [
        related_institution(p, record_id, record_type, i)
        for i, p in enumerate(institutions, 1)
        if p and p.get("0")
    ]


BREAK_CONVERT: Pattern = re.compile(r"({{brk}})")
URL_MATCH: Pattern = re.compile(
    r"((https?):((//)|(\\\\))+[\w\d:#@%/;$()~_?+-=\\.&]*)", re.MULTILINE | re.UNICODE
)
OPAC_LINK: Pattern = re.compile(
    r"https?://opac\.rism\.info/search\?id=(\d+)&View=rism", re.MULTILINE | re.UNICODE
)
MUSCAT_LINK: Pattern = re.compile(
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
    field: pymarc.Field, catalogue_fields: Optional[list[pymarc.Field]]
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
    catalogue_fields: Optional[list[pymarc.Field]],
    holding: Optional[pymarc.Field],
    source_type: Optional[pymarc.Field],
) -> dict:
    catalogue_numbers = get_catalogue_numbers(field, catalogue_fields)

    d = {
        "title": field.get("a"),
        "subheading": field.get("k"),
        "arrangement": field.get("o"),
        "key_mode": field.get("r"),
        "catalogue_numbers": catalogue_numbers,
    }

    scoring_summary_f: str = field.get("m")
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


def get_where(
    record: pymarc.Record, field: str, conditions: dict
) -> list[pymarc.Field]:
    fields: list[pymarc.Field] = record.get_fields(field)
    if not fields:
        return []

    out = []
    for subf, val in conditions.items():
        for marcf in fields:
            if subf in marcf and marcf[subf] == val:
                out.append(marcf)

    return out


def get_titles(record: pymarc.Record, field: str) -> Optional[list[dict]]:
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

    c: Optional[list[pymarc.Field]] = None
    h: Optional[pymarc.Field] = None
    y: Optional[pymarc.Field] = None
    if field == "240":
        c = record.get_fields("383", "690")
        if "852" in record:
            h = record.get("852")

        if "593" in record:
            # If the record has a 593 and that is for material group 01, then
            # prefer that for generating the titles. If it does not,
            # then simply take the first 593.
            candidates = get_where(record, "593", {"8": "01"})
            y = candidates[0] if candidates else record.get("593")

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


def get_creator_name(record: pymarc.Record) -> Optional[str]:
    creator_field: Optional[pymarc.Field] = record.get("100")
    if not creator_field:
        return None

    creator_name: str = creator_field.get("a", "").strip()
    creator_dates: str = f" ({d})" if (d := creator_field.get("d")) else ""
    return f"{creator_name}{creator_dates}"


@dataclasses.dataclass
class ContentTypes:
    NOTATED_MUSIC = "Notated music"
    LIBRETTO = "Libretto"
    TREATISE = "Treatise"
    MIXED = "Mixed"
    OTHER = "Other"


def get_content_types(record: Optional[pymarc.Record]) -> list[str]:
    """
    Takes all record types associated with this record, and returns a list of
    all possible content types for it.

    Checks if two sets have an intersection set (that they have members overlapping).

    :param record: A pymarc Record field
    :return: A list of index values containing the content types.
    """
    if record is None:
        return []

    all_content_types: Optional[list[str]] = to_solr_multi(record, "593", "b")
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

    return ret


def get_parent_order_for_members(
    parent_record: Optional[pymarc.Record], this_id: str
) -> Optional[int]:
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
                f"Problem when searching the membership of {this_id} in {normalize_id(parent_record['001'].value())}."
            )
            continue

        pfx: str = "source_"
        if "4" in field and field["4"] == "holding":
            pfx = "holding_"

        idxs.append(f"{pfx}{normalize_id(subf_id)}")

    if this_id in idxs:
        return idxs.index(this_id)

    return None


def get_bibliographic_reference_titles(
    references: Optional[list[str]],
) -> Optional[list[str]]:
    if not references:
        return None

    ret: list = []
    for r in references:
        # |:| is a unique field delimiter
        _, *rest = r.split("|:|")
        ret.append(format_reference(rest))

    return ret


def get_bibliographic_references_json(
    record: pymarc.Record, field: str, references: Optional[list[str]]
) -> Optional[list[dict]]:
    if not references:
        return None

    if field not in record:
        return None

    refs: dict = {}
    for ref in references:
        # |:| is a unique field delimiter
        rid, *rest = ref.split("|:|")

        try:
            refs[rid] = format_reference(rest)
        except ValueError:
            log.error(
                "Could not index references for record %s.", record["001"].value()
            )
            return None

    outp: list = []
    fields: list[pymarc.Field] = record.get_fields(field)

    for ff in fields:
        fid: Optional[str] = ff.get("0")
        if not fid:
            log.error(
                "No field 0 for entry in record %s. Skipping.", record["001"].value()
            )
            continue

        literature_id: str = f"literature_{fid}"
        lit = {
            "id": literature_id,
            "formatted": refs[fid],
        }
        if p := ff.get("n"):
            lit["pages"] = p

        outp.append(lit)

    return outp


def format_reference(ref: list) -> str:
    author, description, journal, date, place, short = ref
    res: str = ""

    if author:
        res += f"{author.strip()}{' ' if author.endswith('.') else '. '}"

    if description:
        res += f"{description.strip()}{' ' if description.endswith('.') else '. '}"

    if journal:
        res += f"{journal.strip()}, "

    if date:
        res += f"{date.strip()}. "

    if place:
        res += f"{place.strip()} "

    if short:
        res += f"({short.strip()})."

    return res


def update_rism_document(
    record,
    project: str,
    record_type: str,
    label: str,
    cfg: dict,
    additional_fields: Optional[dict] = None,
) -> Optional[dict]:
    document_id: Optional[str] = transform_rism_id(record.get("rism_id"))
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
        "project_type": f'{record.get("project_type")}',
        "project": f"{project}",
        "label": f"{label}",
    }

    if additional_fields:
        entry.update(additional_fields)

    entry_s: str = orjson.dumps(entry).decode("utf-8")

    return {
        "id": document_id,
        "has_external_record_b": {"set": True},
        "external_records_jsonm": {"add-distinct": entry_s},
    }
