import datetime
import itertools
import logging
from collections import defaultdict
from typing import Dict, List, Optional, Tuple

import pymarc

from indexer.helpers.datelib import parse_date_statement
from indexer.helpers.identifiers import country_code_from_siglum
from indexer.helpers.utilities import to_solr_single, normalize_id, to_solr_multi, external_resource_data, \
    to_solr_single_required, get_related_people, get_related_institutions, get_related_places, get_titles, \
    related_person, related_institution, get_catalogue_numbers

log = logging.getLogger("muscat_indexer")

EARLIEST_YEAR_IF_MISSING: int = -2000
LATEST_YEAR_IF_MISSING: int = datetime.datetime.now().year


def _get_has_incipits(record: pymarc.Record) -> bool:
    return '031' in record


def _get_num_incipits(record: pymarc.Record) -> int:
    return len(record.get_fields("031"))


def _get_creator_name(record: pymarc.Record) -> Optional[str]:
    creator: pymarc.Field = record["100"]
    if not creator:
        return None

    name: str = creator["a"].strip()
    dates: str = f" ({d})" if (d := creator["d"]) else ""

    return f"{name}{dates}"


def _get_creator_data(record: pymarc.Record) -> Optional[List]:
    source_id: str = f"source_{normalize_id(to_solr_single_required(record, '001'))}"
    creator = get_related_people(record, source_id, "source", fields=("100",))
    if not creator:
        return None

    creator[0]["relationship"] = "cre"
    return creator


def _get_subjects(record: pymarc.Record) -> Optional[List[Dict]]:
    subject_fields: List[pymarc.Field] = record.get_fields("650")
    if not subject_fields:
        return None

    ret: List = []
    for field in subject_fields:
        d = {
            "id": f"subject_{field['0']}",
            "subject": field["a"]
        }
        # Ensure we remove any None values
        ret.append({k: v for k, v in d.items() if v})

    return ret


def _get_standard_titles_data(record: pymarc.Record) -> Optional[List]:
    return get_titles(record, "240")


def _get_catalogue_numbers(record: pymarc.Record) -> Optional[List]:
    # Catalogue numbers are spread across a number of fields, including 'opus numbers'
    # (383) and 'catalogue of works' (690), where the catalogue and the catalogue
    # entry are held in different subfields. This function consolidates both of those fields,
    # and unites the separate subfields into a single set of identifiers so that we can search on
    # all of them. The 'get_catalogue_numbers' function depends on having access to the
    # 240 field entry for the correct behaviour, so we also pass this in, even though
    # it doesn't hold any data for the catalogue numbers directly.
    title_fields: List = record.get_fields("240")
    if not title_fields:
        return None

    catalogue_record_fields: Optional[List[pymarc.Field]] = record.get_fields("383", "690")
    if not catalogue_record_fields:
        return None

    catalogue_nums: List = get_catalogue_numbers(title_fields[0], catalogue_record_fields)

    return catalogue_nums


def _get_scoring_summary(record: pymarc.Record) -> Optional[List]:
    """Takes a list of instrument fields and ensures that they are split into a multi-valued representation. So a
       value of:
       ["V, orch", "B, guit"]

       Would result in an instrument list of:

       ["V", "orch", "B", "guit"]
       """
    fields: Optional[List] = to_solr_multi(record, "240", "m")
    if not fields:
        return None

    all_instruments: List = list({val.strip() for field in fields for val in field.split(",") if val and val.strip()})
    return all_instruments


def _get_is_arrangement(record: pymarc.Record) -> bool:
    fields: Optional[List] = record.get_fields("240")
    if not fields:
        return False
    valid_statements: Tuple = ("Arr", "arr", "Arrangement")
    # if any 240 field has it, we mark the whole record as an arrangement.
    for field in fields:
        if 'o' in field and field['o'] in valid_statements:
            return True
    return False


def _get_earliest_latest_dates(record: pymarc.Record) -> Optional[List[int]]:
    date_statements: Optional[List] = to_solr_multi(record, "260", "c")
    if not date_statements:
        return None

    earliest_dates: List[int] = []
    latest_dates: List[int] = []

    for statement in date_statements:
        if not statement or statement in ("[s.a.]", "[s. a.]", "s/d", "n/d", "(s.d.)", "[s.d.]", "[s.d]", "[s. d.]", "s. d.", "s.d.", "[n.d.]", "n. d.", "n.d.", "[n. d.]", "[o.J]", "o.J", "o.J.", "[s.n.]", "(s. d.)"):
            continue

        try:
            earliest, latest = parse_date_statement(statement)
        except Exception as e:  # noqa
            # The breadth of errors mean we could spend all day catching things, so in this case we use
            # a blanket exception catch and then log the statement to be fixed so that we might fix it later.
            log.error("Error parsing date statement %s: %s", statement, e)
            return None

        if earliest is None and latest is None:
            source_id: str = to_solr_single_required(record, '001')
            log.warning("Problem with date statement %s for source %s", statement, source_id)
            return None

        if earliest:
            earliest_dates.append(earliest)

        if latest:
            latest_dates.append(latest)

    # To prevent things like 18,345 AD, choose the min value of the latest
    # date and the current year (which is what we set it to if it's missing).
    # To do this for the earliest date, choose the min value of all dates
    # discovered in the source, then choose the max value between that and
    # the earliest.
    earliest_date: int = max(min(earliest_dates), EARLIEST_YEAR_IF_MISSING) if earliest_dates else EARLIEST_YEAR_IF_MISSING
    latest_date: int = min(max(latest_dates), LATEST_YEAR_IF_MISSING) if latest_dates else LATEST_YEAR_IF_MISSING

    # If neither date was parseable, don't pretend we have a date.
    if earliest_date == EARLIEST_YEAR_IF_MISSING and latest_date == LATEST_YEAR_IF_MISSING:
        return None

    # If we don't have one but we do have the other, these will still be valid (but
    # improbable) integer values.
    return [earliest_date, latest_date]


def _get_rism_series_identifiers(record: pymarc.Record) -> Optional[List]:
    fields: List[pymarc.Field] = record.get_fields("510")
    if not fields:
        return None

    ret: List = []

    for field in fields:
        stmt: str = ""
        if series := field['a']:
            stmt += series
        if ident := field['c']:
            stmt += f" {ident}"

        if stmt:
            ret.append(stmt)

    return ret


def _get_source_notes_data(record: pymarc.Record) -> Optional[List[Dict]]:
    # Get all notes fields that we're interested in.
    fields: List[pymarc.Field] = record.get_fields("500", "505", "518", "520", "561", "")


def __scoring(field: pymarc.Field) -> Dict:
    d = {
        "voice_instrument": field["b"],
        "number": field["c"]
    }

    return {k: v for k, v in d.items() if v}


def _get_scoring_data(record: pymarc.Record) -> Optional[List[Dict]]:
    fields: List = record.get_fields("594")
    if not fields:
        return None

    return [__scoring(i) for i in fields]


def _get_dramatic_roles_data(record: pymarc.Record) -> Optional[List[Dict]]:
    fields: List[pymarc.Field] = record.get_fields("595")
    if not fields:
        return None

    ret: List = []
    for field in fields:
        d = {
            "standard_spelling": field['a'],
            "source_spelling": field['u']
        }
        ret.append({k: v for k, v in d.items() if v})

    return ret


def _get_rism_series_data(record: pymarc.Record) -> Optional[List[Dict]]:
    fields: List[pymarc.Field] = record.get_fields("596")
    if not fields:
        return None

    ret: List = []
    for field in fields:
        d = {
            "reference": field['a'],
            "series_id": field['b']
        }
        ret.append({k: v for k, v in d.items() if v})

    return ret


def _get_location_performance_data(record: pymarc.Record) -> Optional[List]:
    source_id: str = f"source_{normalize_id(to_solr_single_required(record, '001'))}"
    places = get_related_places(record, source_id, "source", fields=("651",))
    if not places:
        return None

    return places


def __liturgical_festival(field: pymarc.Field) -> Dict:
    d = {
        "id": f"festival_{field['0']}",
        "name": f"{field['a']}"
    }
    return {k: v for k, v in d.items() if v}


def _get_liturgical_festival_data(record: pymarc.Record) -> Optional[List[Dict]]:
    fields: List = record.get_fields("657")
    if not fields:
        return None

    return [__liturgical_festival(f) for f in fields]


def __secondary_literature_data(field: pymarc.Field) -> Dict:
    d = {
        "id": f"literature_{field['0']}",  # not used, but stored for now.
        "reference": field['a'],
        "number_page": field['n']
    }
    return {k: v for k, v in d.items() if v}


def _get_works_catalogue_data(record: pymarc.Record) -> Optional[List[Dict]]:
    fields: List[pymarc.Field] = record.get_fields("690")
    if not fields:
        return None

    return [__secondary_literature_data(f) for f in fields]


def _get_bibliographic_reference_data(record: pymarc.Record) -> Optional[List[Dict]]:
    fields: List[pymarc.Field] = record.get_fields("691")
    if not fields:
        return None

    return [__secondary_literature_data(f) for f in fields]


def _get_secondary_literature_identifiers(record: pymarc.Record) -> Optional[List]:
    fields: List[pymarc.Field] = record.get_fields("691")
    if not fields:
        return None

    ret: List = []
    for field in fields:
        stmt: str = ""
        if ref := field['a']:
            stmt += ref
        if num := field['n']:
            stmt += f" {num}"

        # ensure we strip leading and trailing spaces
        if chomped := stmt.strip():
            ret.append(chomped)

    return ret


def _get_related_people_data(record: pymarc.Record) -> Optional[List]:
    source_id: str = f"source_{normalize_id(to_solr_single_required(record, '001'))}"
    people = get_related_people(record, source_id, "source", fields=("700",), ungrouped=True)
    if not people:
        return None

    return people


def _get_related_institutions_data(record: pymarc.Record) -> Optional[List]:
    source_id: str = f"source_{normalize_id(to_solr_single_required(record, '001'))}"
    institutions = get_related_institutions(record, source_id, "source", fields=("710",))
    if not institutions:
        return None

    return institutions


def _get_additional_titles_data(record: pymarc.Record) -> Optional[List]:
    return get_titles(record, "730")


def _get_source_membership(record: pymarc.Record) -> Optional[List]:
    members: Optional[List] = record.get_fields("774")
    if not members:
        return None

    ret: List = []

    for tag in members:
        member_id: Optional[str] = tag["w"] or None
        if not member_id:
            continue

        member_type: Optional[str] = tag["4"] or None
        # Create an ID like "holding_12345" or "source_4567" (default)
        ret.append(
            f"{'source' if not member_type else member_type}_{normalize_id(member_id)}"
        )

    return ret


def _get_country_code(record: pymarc.Record) -> Optional[str]:
    siglum: Optional[str] = to_solr_single(record, "852", "a")
    if not siglum:
        return None

    return country_code_from_siglum(siglum)


def _get_minimal_manuscript_holding_data(record: pymarc.Record) -> Optional[List]:
    """
    A minimal holdings record suitable for indexing directly on the source. Only used for manuscript holdings
    so that we can link the holding institution and shelfmark directly in the record. For all other uses, the
    full holdings records should be used!

    Only included when there is an 852 field directly attached to the record.

    :param record: A pymarc record
    :return: A dictionary containing enough information to link the holding institution and shelfmark.
    """
    fields: List[pymarc.Field] = record.get_fields("852")
    if not fields:
        return None

    ret: List = []
    for field in fields:
        d = {
            "siglum": field['a'],
            "holding_institution_name": field['e'],
            "holding_institution_id": f"institution_{field['x']}"
        }
        filtd: Dict = {k: v for k, v in d.items() if v}
        ret.append(filtd)

    return ret


def _get_external_resources_data(record: pymarc.Record) -> Optional[List]:
    """
    Fetch the external links defined on the record. Note that this will *not* index the links that are linked to
    material group descriptions -- those are handled in the material group indexing section above.
    :param record: A pymarc record
    :return: A list of external links. This will be serialized to a string for storage in Solr.
    """
    ungrouped_ext_links: List = [external_resource_data(f) for f in record.get_fields("856") if f and ('8' not in f or f['8'] != 0)]
    if not ungrouped_ext_links:
        return None

    return ungrouped_ext_links


def _get_has_digitization(record: pymarc.Record) -> bool:
    digitization_links: List = [f for f in record.get_fields("856") if 'x' in f and f['x'] == "Digitalization"]

    return len(digitization_links) > 0


def _get_has_iiif_manifest(record: pymarc.Record) -> bool:
    iiif_manifests: List = [f for f in record.get_fields("856") if 'x' in f and f['x'] == "IIIF"]

    return len(iiif_manifests) > 0


# Material Group Handling
# Forward-declare some typed dictionaries. These both help to ensure the documents getting indexed
# contain the expected fields of the expected types, and serve as a point of reference to know
# what fields are on what type of record in Solr.
MaterialGroupFields = Dict[str, List]


def __mg_plate(field: pymarc.Field) -> MaterialGroupFields:
    # 028 can be either publisher number (30) or plate number (20), depending on the indicators
    # The default assumption is that it is a plate number, since this was the only value
    # available until 06/2021.
    field_name: str = "publisher_numbers" if field.indicator1 == "3" else "plate_numbers"

    res: MaterialGroupFields = {
        field_name: field.get_subfields('a')
    }

    return res


def __mg_pub(field: pymarc.Field) -> MaterialGroupFields:
    # 260
    res: MaterialGroupFields = {
        "publication_place": field.get_subfields("a"),
        "publisher_copyist": field.get_subfields("b"),
        "date_statements": field.get_subfields("c"),
        "printer_location": field.get_subfields("e"),
        "printer_name": field.get_subfields("f")
    }

    return res


def __mg_phys(field: pymarc.Field) -> MaterialGroupFields:
    # 300
    res: MaterialGroupFields = {
        "physical_extent": field.get_subfields("a"),
        "physical_details": field.get_subfields("b"),
        "physical_dimensions": field.get_subfields("c")
    }

    return res


def __mg_special(field: pymarc.Field) -> MaterialGroupFields:
    # 340
    # 340$a is deprecated, but at this time most data is in that
    # subfield, so we get values from both fields.
    # TODO: Remove $a when this is fixed in muscat.
    res: MaterialGroupFields = {
        "printing_techniques": field.get_subfields("a", "d"),
        "book_formats": field.get_subfields("m")
    }

    return res


def __mg_general(field: pymarc.Field) -> MaterialGroupFields:
    # 500
    note_values: List[str] = field.get_subfields("a")
    notes: list[str] = __process_brk(note_values)

    res: MaterialGroupFields = {
        "general_notes": notes
    }

    return res


def __mg_binding(field: pymarc.Field) -> MaterialGroupFields:
    # 563
    note_values = field.get_subfields("a")
    notes = __process_brk(note_values)

    res: MaterialGroupFields = {
        "binding_notes": notes
    }

    return res


def __mg_parts(field: pymarc.Field) -> MaterialGroupFields:
    # 590
    res: MaterialGroupFields = {
        "parts_held": field.get_subfields('a'),
        "parts_extent": e if (e := field.get_subfields('b')) else []
    }
    return res


def __mg_watermark(field: pymarc.Field) -> MaterialGroupFields:
    # 592
    note_values = field.get_subfields("a")
    notes = __process_brk(note_values)

    res: MaterialGroupFields = {
        "watermark_notes": notes
    }
    return res


def __mg_type(field: pymarc.Field) -> MaterialGroupFields:
    # 593
    res: MaterialGroupFields = {
        "material_group_types": field.get_subfields('a')
    }

    return res


def __mg_add_name(field: pymarc.Field) -> MaterialGroupFields:
    # 700
    # We pass some dummy values to the related_person function
    # so that we can keep the structure of the data consistent.
    person = related_person(field, "", "material_group", 0)

    # Use the same key in the output so that we can use the
    # same relationship serializer as we do for the full object.
    res: MaterialGroupFields = {
        "related_people_json": [person]
    }

    return res


def __mg_add_inst(field: pymarc.Field) -> MaterialGroupFields:
    # 710
    institution = related_institution(field, "", "material_group", 0)

    res: MaterialGroupFields = {
        "related_institutions_json": [institution]
    }

    return res


def __mg_external(field: pymarc.Field) -> MaterialGroupFields:
    # 856
    res: MaterialGroupFields = {
        "external_resources": [external_resource_data(field)]
    }
    return res


def _get_material_groups(record: pymarc.Record) -> Optional[List[Dict]]:
    """
    To get all the material groups we must first find all the members of each group, and then
    process the individual fields belonging to that group. Every source should always have a group
    01, and some have multiple material groups numbered 02, 03, etc.

    First we define a mapping between the fields and a function that will process that field. Each
    of these functions will return a dictionary containing one or more fields to add to the material
    group, depending on the contents of that field. These fields will be added to the 'main' material
    group record.

    Then we fetch the fields from the records and sort them into a dictionary grouped by their field.

    Then for each group we create the material group dictionary by iterating over the fields and creating
    the MaterialGroupIndexDocument from the contents of the available fields. Each material group is then
    appended to a list that is returned and stored on the source record.

    :param record: A pymarc.Record instance
    :return: A list of MaterialGroupIndexDocument instances
    """
    log.debug("Indexing material groups")
    source_id: str = f"source_{normalize_id(to_solr_single_required(record, '001'))}"

    # Set the mapping between the MARC field and a function to handle processing that field
    # for the material group. Each function takes the field as the only argument, producing
    # a dictionary of one or more fields to be sent to Solr.
    member_fields: Dict = {
        "028": __mg_plate,
        "260": __mg_pub,
        "300": __mg_phys,
        "340": __mg_special,
        "500": __mg_general,
        "563": __mg_binding,
        "590": __mg_parts,
        "592": __mg_watermark,
        "593": __mg_type,
        "700": __mg_add_name,
        "710": __mg_add_inst,
        "856": __mg_external
    }

    # Filter any field instances that do not declare themselves part of a group ($8). This is
    # important especially for the fields that can occur on both the main record and in the
    # material group records, e.g., 700, 710, 856.
    field_instances: List[pymarc.Field] = [f for f in record.get_fields(*member_fields.keys()) if '8' in f]

    if not field_instances:
        return None

    # groupby needs the data to be pre-sorted.
    data = sorted(field_instances, key=lambda f: str(f['8']))
    field_groups: List = []

    # Organizes the fields by material groups. Creates a tuple of (groupnum, [fields...]) and appends
    # it to the field groups list.
    for k, g in itertools.groupby(data, key=lambda f: str(f['8'])):
        field_groups.append((k, list(g)))

    res: List = []
    for gpnum, fields in field_groups:
        # The field group will be all multivalued fields, and the base group
        # will be single-valued fields. These will be merged later to form
        # the whole group. We use a defaultdict here so that we can just assume
        # that the fields will always be a list, and we don't have to check whether
        # it exists before adding new values to it.
        field_group = defaultdict(list)
        base_group: Dict = {
            "id": f"mg_{gpnum}",
            "group_num": f"{gpnum}",
            "source_id": source_id
        }

        for field in fields:
            field_res: MaterialGroupFields = member_fields[field.tag](field)
            if not field_res:
                continue

            for subf, subv in field_res.items():
                # Filter out any empty values
                if not subv:
                    continue
                # Extend any previous values with the new values
                field_group[subf].extend(subv)

        # Join the field group to the base group, and then append the combined dict
        # to the list of results to be indexed. The set
        base_group.update(field_group)
        res.append(base_group)

    return res


def __process_brk(note_values: list[str]) -> list[str]:
    notes: list = []
    for note in note_values:
        # the '{{brk}}' string is used in some notes to signify a line break.
        new_note = note.split("{{brk}}")
        notes += new_note

    return notes
