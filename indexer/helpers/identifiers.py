from enum import unique, IntEnum
from typing import Optional


@unique
class RecordTypes(IntEnum):
    UNSPECIFIED = 0
    COLLECTION = 1
    SOURCE = 2
    EDITION_CONTENT = 3
    LIBRETTO_SOURCE = 4
    LIBRETTO_EDITION = 5
    THEORETICA_SOURCE = 6
    THEORETICA_EDITION = 7
    EDITION = 8
    LIBRETTO_EDITION_CONTENT = 9
    THEORETICA_EDITION_CONTENT = 10
    COMPOSITE_VOLUME = 11


RECORD_TYPES: dict = {
    "unspecified": RecordTypes.UNSPECIFIED,
    "collection": RecordTypes.COLLECTION,
    "source": RecordTypes.SOURCE,
    "edition_content": RecordTypes.EDITION_CONTENT,
    "libretto_source": RecordTypes.LIBRETTO_SOURCE,
    "libretto_edition": RecordTypes.LIBRETTO_EDITION,
    "theoretica_source": RecordTypes.THEORETICA_SOURCE,
    "theoretica_edition": RecordTypes.THEORETICA_EDITION,
    "edition": RecordTypes.EDITION,
    "libretto_edition_content": RecordTypes.LIBRETTO_EDITION_CONTENT,
    "theoretica_edition_content": RecordTypes.THEORETICA_EDITION_CONTENT,
    "composite_volume": RecordTypes.COMPOSITE_VOLUME,
}

# Invert record types table for lookups
RECORD_TYPES_BY_ID: dict = {v: k for k, v in RECORD_TYPES.items()}


def get_record_type(record_type_id: int) -> str:
    if record_type_id in (
            RecordTypes.COLLECTION,
            RecordTypes.EDITION,
            RecordTypes.LIBRETTO_EDITION,
            RecordTypes.THEORETICA_EDITION
    ):
        return "collection"
    elif record_type_id in (
            RecordTypes.COMPOSITE_VOLUME,
    ):
        return "composite"
    else:
        return "item"


def get_source_type(record_type_id: int) -> str:
    if record_type_id in (
            RecordTypes.EDITION,
            RecordTypes.EDITION_CONTENT,
            RecordTypes.LIBRETTO_EDITION,
            RecordTypes.THEORETICA_EDITION,
            RecordTypes.LIBRETTO_EDITION_CONTENT,
            RecordTypes.THEORETICA_EDITION_CONTENT
    ):
        return "printed"
    elif record_type_id in (
            RecordTypes.COLLECTION,
            RecordTypes.SOURCE,
            RecordTypes.LIBRETTO_SOURCE,
            RecordTypes.THEORETICA_SOURCE
    ):
        return "manuscript"
    elif record_type_id in (
            RecordTypes.COMPOSITE_VOLUME,
    ):
        return "composite"
    else:
        return "unspecified"


def get_content_types(record_type_id: int, child_record_types: list[int]) -> list[str]:
    """
    Takes all record types associated with this record, and returns a list of
    all possible content types for it.

    Checks if two sets have an intersection set (that they have members overlapping).

    :param record_type_id: The record type id of the source record
    :param child_record_types: The record type ids of all child records
    :return: A list of index values containing the content types.
    """
    all_types: set = set([record_type_id] + child_record_types)
    ret: list = []

    if all_types & {RecordTypes.LIBRETTO_EDITION_CONTENT,
                    RecordTypes.LIBRETTO_EDITION,
                    RecordTypes.LIBRETTO_SOURCE}:
        ret.append("libretto")

    if all_types & {RecordTypes.THEORETICA_EDITION_CONTENT,
                    RecordTypes.THEORETICA_EDITION,
                    RecordTypes.THEORETICA_SOURCE}:
        ret.append("treatise")

    if all_types & {RecordTypes.SOURCE,
                    RecordTypes.EDITION,
                    RecordTypes.EDITION_CONTENT}:
        ret.append("musical")

    if all_types & {RecordTypes.COMPOSITE_VOLUME}:
        ret.append("composite_content")

    return ret


def get_is_contents_record(record_type_id: int, parent_id: Optional[int]) -> bool:
    if record_type_id in (
            RecordTypes.EDITION_CONTENT,
            RecordTypes.LIBRETTO_EDITION_CONTENT,
            RecordTypes.THEORETICA_EDITION_CONTENT
    ):
        return True
    elif record_type_id in (
            RecordTypes.SOURCE,
            RecordTypes.LIBRETTO_SOURCE,
            RecordTypes.THEORETICA_SOURCE
    ) and parent_id is not None:
        return True
    else:
        return False


def get_is_collection_record(record_type_id: int, children_count: int) -> bool:
    if record_type_id in (
            RecordTypes.COLLECTION,
            RecordTypes.LIBRETTO_SOURCE,
            RecordTypes.LIBRETTO_EDITION,
            RecordTypes.THEORETICA_SOURCE,
            RecordTypes.THEORETICA_EDITION
    ) and children_count > 0:
        return True
    return False


def country_code_from_siglum(siglum: str) -> str:
    # split the country code from the rest of the siglum, and return that.
    # If there was a problem splitting the siglum because it was malformed,
    # return it wholescale and keep going.
    split_sig = siglum.split("-")
    return split_sig[0] if len(split_sig) > 0 else siglum


# Until the data is cleaned up in Muscat, we can map these letters to
# their appropriate values. This data comes from the old Kalliope database,
# and the letters were used to indicate the following values:
# K = Körperschaft
# B = Bibliothek
# V = Verleger
# C = Kongress
# F = Forschungsinstitut
#
# These then map to the "accepted" muscat values as follows.
KALLIOPE_MAPPING = {
    "K": "Institution",
    "B": "Library",
    "V": "Publisher",
    "C": "Congress",
    "F": "Research institute",
}

COUNTRY_CODE_MAPPING = {
    "A": ["Austria", "L'Autriche", "Österreich", ],
    "AFG": ["Afghanistan", "Afeganistão", "Afganistán"],
    "AND": ["Andorra"],
    "ARM": ["Armenia", "Arménie"],
    "AS": ["Saudi Arabia"],
    "AUS": ["Australia"],
    "AZ": ["Azerbaijan"],
    "B": ["Belgium"],
    "BD": ["Bangladesh"],
    "BG": ["Bulgaria"],
    "BIH": ["Bosnia and Herzegovina"],
    "BOL": ["Bolivia"],
    "BR": ["Brazil"],
    "BY": ["Belarus"],
    "C": ["Cuba"],
    "CDN": ["Canada", "Kanada"],
    "CH": ["Switzerland", "Schweiz", "Svizzera", "Suíça", "Suiza", "Szwajcaria"],
    "CN": ["China"],
    "CO": ["Colombia"],
    "CR": ["Costa Rica"],
    "CZ": ["Czechoslovakia", "Czech Republic"],
    "D": ["Germany", "Deutschland", "Allemagne", "Germania", "Alemania", "Alemanha", "Niemcy"],
    "DK": ["Denmark"],
    "DY": ["Benin"],
    "DZ": ["Algeria"],
    "E": ["Spain", "Espagne", "Spanien", "Spagna", "España", "Hiszpania", "Espanha"],
    "EC": ["Ecuador"],
    "ET": ["Egypt"],
    "EV": ["Estonia"],
    "F": ["France"],
    "FIN": ["Finland"],
    "GB": ["United Kingdom", "Great Britain", "Royaume-Uni", "Vereinigtes Königreich", "Regno Unito", "Reino Unido", "Zjednoczone Królestwo"],
    "GCA": ["Guatemala"],
    "GE": ["Georgia"],
    "GR": ["Greece"],
    "H": ["Hungary"],
    "HK": ["Hong Kong"],
    "HN": ["Honduras"],
    "HR": ["Croatia"],
    "I": ["Italy", "Italie", "Italien", "Italia", "Włochy"],
    "IL": ["Israel"],
    "IND": ["India"],
    "IR": ["Iran"],
    "IRL": ["Ireland"],
    "IRLN": ["Northern Ireland"],
    "IS": ["Iceland"],
    "J": ["Japan"],
    "K": ["Cambodia"],
    "L": ["Luxembourg"],
    "LT": ["Lithuania"],
    "LV": ["Latvia"],
    "M": ["Malta"],
    "MC": ["Monaco"],
    "MD": ["Moldova"],
    "MEX": ["Mexico"],
    "MNE": ["Montenegro"],
    "N": ["Norway"],
    "NIC": ["Nicaragua"],
    "NL": ["Netherlands"],
    "NMK": ["North Macedonia"],
    "NZ": ["New Zealand"],
    "P": ["Portugal"],
    "PE": ["Peru"],
    "PK": ["Pakistan"],
    "PL": ["Poland"],
    "PNG": ["Papua New Guinea"],
    "PRI": ["Puerto Rico"],
    "PY": ["Paraguay"],
    "RA": ["Argentina"],
    "RC": ["Republic of China"],
    "RCH": ["Chile"],
    "RI": ["Indonesia"],
    "RL": ["Lebanon"],
    "RO": ["Romania"],
    "ROK": ["Republic of Korea"],
    "ROU": ["Uruguay"],
    "RP": ["Philippines"],
    "RUS": ["Russia"],
    "S": ["Sweden"],
    "SI": ["Slovenia"],
    "SK": ["Slovakia"],
    "SRB": ["Serbia"],
    "SYR": ["Syria"],
    "TA": ["Tajikistan"],
    "TR": ["Turkey"],
    "TT": ["Trinidad and Tobago"],
    "UA": ["Ukraine"],
    "US": ["America", "United States", "États Unis", "Stany Zjednoczone AP", "Vereinigte Staaten", "Amerika", "Estados Unidos", "Stati Uniti"],
    "USB": ["Uzbekistan"],
    "UY": ["Uruguay"],
    "V": ["Vatican", "Vatikan", "Vaticano", "Watykan", "Holy See"],
    "VE": ["Venezuela"],
    "VN": ["Vietname"],
    "XX": [],
    "ZA": ["South Africa"]
}


ISO3166_TO_SIGLUM_MAPPING = {
    "XA-AT": "A",
    "XA-AT-2": "A",
    "XA-AT-3": "A",
    "XA-AT-4": "A",
    "XA-AT-5": "A",
    "XA-AT-6": "A",
    "XA-AT-7": "A",
    "XA-AT-9": "A",
    "XA-BE": "B",
    "XA-BG": "BG",
    "XA-CH": "CH",
    "XA-CH-VD": "CH",
    "XA-CZ": "CZ",
    "XA-DE": "D",
    "XA-DE-BY": "D",
    "XA-DE-SN": "D",
    "XA-DK": "DK",
    "XA-EE": "EV",
    "XA-ES": "E",
    "XA-FI": "FIN",
    "XA-FR": "F",
    "XA-GB": "GB",
    "XA-GB-NIR": "IRLN",
    "XA-GR": "GR",
    "XA-HR": "HR",
    "XA-HU": "H",
    "XA-IE": "IRL",
    "XA-IS": "IS",
    "XA-IT": "I",
    "XA-IT-32": "I",
    "XA-LT": "LT",
    "XA-LU": "L",
    "XA-LV": "LV",
    "XA-MC": "MC",
    "XA-ME": "MNE",
    "XA-MT": "M",
    "XA-NL": "NL",
    "XA-NO": "N",
    "XA-PL": "PL",
    "XA-PT": "P",
    "XA-RO": "RO",
    "XA-RS": "SRB",
    "XA-RU": "RUS",
    "XA-SE": "S",
    "XA-SI": "SI",
    "XA-SK": "SK",
    "XA-UA": "UA",
    "XA-VA": "V",
    "XB-AM": "ARM",
    "XB-CN": "CN",
    "XB-HK": "HK",
    "XB-ID": "RI",
    "XB-IL": "IL",
    "XB-IN": "IND",
    "XB-IR": "IR",
    "XB-JP": "J",
    "XB-KH": "K",
    "XB-KR": "ROK",
    "XB-PH": "RP",
    "XB-SA": "AS",
    "XB-SY": "SYR",
    "XB-TR": "TR",
    "XB-TW": "RC",
    "XB-VN": "VN",
    "XC-BJ": "DY",
    "XC-DZ": "DZ",
    "XC-EG": "ET",
    "XC-ZA": "ZA",
    "XD-AR": "RA",
    "XD-BR": "BR",
    "XD-CA": "CDN",
    "XD-CL": "RCH",
    "XD-CO": "CO",
    "XD-CU": "C",
    "XD-EC": "EC",
    "XD-GT": "GCA",
    "XD-HN": "HN",
    "XD-MX": "MEX",
    "XD-PR": "PRI",
    "XD-PY": "PY",
    "XD-TT": "TT",
    "XD-US": "US",
    "XD-UY": "UY",
    "XD-VE": "VE",
    "XE-AU": "AUS",
    "XE-NZ": "NZ",
    "XE-PG": "PNG",
}
