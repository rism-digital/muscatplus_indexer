import itertools
import logging
from collections import defaultdict
from typing import Dict, List, Optional, TypedDict

import pymarc as pymarc

from indexer.helpers.datelib import parse_date_statement
from indexer.helpers.identifiers import country_code_from_siglum
from indexer.helpers.utilities import to_solr_single, normalize_id, to_solr_multi, external_resource_data, \
    to_solr_single_required, get_related_people, get_related_institutions, get_related_places

log = logging.getLogger("muscat_indexer")


def _get_creator_name(record: pymarc.Record) -> Optional[str]:
    creator: pymarc.Field = record["100"]
    if not creator:
        return None

    name: str = creator["a"]
    dates: str = f" ({d})" if (d := to_solr_single(record, "100", "d")) else ""

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


def _get_earliest_latest_dates(record: pymarc.Record) -> Optional[List[int]]:
    earliest_dates: List[int] = []
    latest_dates: List[int] = []
    date_statements: Optional[List] = to_solr_multi(record, "260", "c")

    # if no date statement, return an empty dictionary. This allows us to keep a consistent return type
    # since a call to `.update()` with an empty dictionary won't do anything.
    if not date_statements:
        return None

    for statement in date_statements:
        try:
            earliest, latest = parse_date_statement(statement)
        except Exception as e:  # noqa
            # The breadth of errors mean we could spend all day catching things, so in this case we use
            # a blanket exception catch and then log the statement to be fixed so that we might fix it later.
            log.error("Error parsing date statement %s: %s", statement, e)
            raise

        if earliest:
            earliest_dates.append(earliest)

        if latest:
            latest_dates.append(latest)

    earliest_date: int = min(earliest_dates) if earliest_dates else -9999
    latest_date: int = max(latest_dates) if latest_dates else 9999

    # If neither date was parseable, don't pretend we have a date.
    if earliest_date == -9999 and latest_date == 9999:
        return None

    return [earliest_date, latest_date]


def __instrumentation(field: pymarc.Field) -> Dict:
    d = {
        "voice_instrument": field["b"],
        "number": field["c"]
    }

    return {k: v for k, v in d.items() if v}


def _get_instrumentation_data(record: pymarc.Record) -> Optional[List[Dict]]:
    fields: List = record.get_fields("594")
    if not fields:
        return None

    return [__instrumentation(i) for i in fields]


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


def _get_related_people_data(record:pymarc.Record) -> Optional[List]:
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
    fields: List = record.get_fields("730")
    if not fields:
        return None

    ret: List = []
    for field in fields:
        d = {
            "additional_title": field['a'],
            "subheading": field['k'],
            "arrangement": field['o'],
            "key_mode": field['r'],
            "catalogue_number": field['n'],
            "scoring_summary": field['m']
        }
        ret.append({k: v for k, v in d.items() if v})

    return ret


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


class MaterialGroupIndexDocument(TypedDict, total=False):
    id: str
    type: str
    source_id: str
    group_num: str
    parts_held: Optional[List[str]]
    extent: Optional[List[str]]
    source_type: Optional[List[str]]
    plate_numbers: Optional[List[str]]


def __mg_plate(field: pymarc.Field) -> MaterialGroupFields:
    res: MaterialGroupFields = {
        "plate_numbers": field.get_subfields('a')
    }

    return res


def __mg_pub(field: pymarc.Field) -> MaterialGroupFields:
    res: MaterialGroupFields = {
        "place_publication": field.get_subfields("a"),
        "name_publisher": field.get_subfields("b"),
        "date_statements": field.get_subfields("c")
    }

    return res


def __mg_phys(field: pymarc.Field) -> MaterialGroupFields:
    res: MaterialGroupFields = {
        "physical_extent": field.get_subfields("a")
    }

    return res


def __mg_special(field: pymarc.Field) -> MaterialGroupFields:
    res: MaterialGroupFields = {
        "printing_techniques": field.get_subfields("d"),
        "book_formats": field.get_subfields("m")
    }
    # 340$a is deprecated and will not appear in the results.

    return res


def __mg_general(field: pymarc.Field) -> MaterialGroupFields:
    res: MaterialGroupFields = {
        "general_notes": field.get_subfields("a")
    }

    return res


def __mg_binding(field: pymarc.Field) -> MaterialGroupFields:
    res: MaterialGroupFields = {
        "binding_notes": field.get_subfields("a")
    }

    return res


def __mg_parts(field: pymarc.Field) -> MaterialGroupFields:
    res: MaterialGroupFields = {
        "parts_held": field.get_subfields('a'),
        "parts_extent": e if (e := field.get_subfields('b')) else []
    }
    return res


def __mg_watermark(field: pymarc.Field) -> MaterialGroupFields:
    res: MaterialGroupFields = {
        "watermark_notes": field.get_subfields("a")
    }
    return res


def __mg_type(field: pymarc.Field) -> MaterialGroupFields:
    res: MaterialGroupFields = {
        "source_type": field.get_subfields('a')
    }

    return res


def __mg_add_name(field: pymarc.Field) -> MaterialGroupFields:
    person: Dict = {}

    if n := field["a"]:
        person["name"] = n

    if i := field['0']:
        person["other_person_id"] = f"person_{normalize_id(i)}"

    if r := field["4"]:
        person["relationship"] = r

    if q := field["j"]:
        person["qualifier"] = q

    res: MaterialGroupFields = {
        "people": [person]
    }

    return res


def __mg_add_inst(field: pymarc.Field) -> MaterialGroupFields:
    institution: Dict = {}

    if n := field['a']:
        institution["name"] = n

    if d := field['b']:
        institution["department"] = d

    if i := field['0']:
        institution["institution_id"] = f"institution_{normalize_id(i)}"

    if q := field['j']:
        institution["qualifier"] = q

    if r := field['4']:
        institution["relationship"] = r

    res: MaterialGroupFields = {
        "institutions": [institution]
    }

    return res


def __mg_external(field: pymarc.Field) -> MaterialGroupFields:
    res: MaterialGroupFields = {
        "external_resources": [external_resource_data(field)]
    }
    return res


def _get_material_groups(record: pymarc.Record) -> Optional[List[MaterialGroupIndexDocument]]:
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
    field_instances: List[pymarc.Field] = [f for f in record.get_fields(*member_fields.keys()) if f['8']]

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
        base_group: MaterialGroupIndexDocument = {
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
                field_group[subf].extend(subv)

        # Cast the sets to a list so that they can be serialized by the JSON encoder.
        # values_list: Dict = {k: list(v) for k, v in field_group.items()}

        # Join the field group to the base group, and then append the combined dict
        # to the list of results to be indexed.
        base_group.update(field_group)
        res.append(base_group)

    return res
