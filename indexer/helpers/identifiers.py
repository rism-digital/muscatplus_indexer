from enum import IntEnum, unique


@unique
class WorkPublicationStatusIdentifiers(IntEnum):
    ALTERNATE = 4
    COMPLETED = 3
    PARTIALLY_COMPLETED = 2
    ELIGIBLE = 1
    NOT_A_CATALOG = 0


class ProjectIdentifiers:
    DIAMM = "diamm"
    CANTUS = "cantus"
    RISM = "rism"


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
    INVENTORY = 12
    INVENTORY_EDITION = 13
    WORK = 99  # Special case, so we can index record types within Incipits


def get_record_type(record_type_id: int, is_single_item: bool) -> str:
    if (
        record_type_id
        in (
            RecordTypes.SOURCE,
            RecordTypes.EDITION,
            RecordTypes.INVENTORY,
            RecordTypes.THEORETICA_EDITION,
            RecordTypes.LIBRETTO_EDITION,
            RecordTypes.INVENTORY_EDITION,
        )
        and is_single_item is True
    ):
        return "single_item"
    elif record_type_id in (
        RecordTypes.COLLECTION,
        RecordTypes.EDITION,
        RecordTypes.LIBRETTO_EDITION,
        RecordTypes.THEORETICA_EDITION,
    ):
        return "collection"
    elif record_type_id == RecordTypes.COMPOSITE_VOLUME:
        return "composite"
    elif record_type_id == RecordTypes.WORK:
        return "work"
    else:
        return "item"


def get_source_type(record_type_id: int) -> str:
    if record_type_id in (
        RecordTypes.EDITION,
        RecordTypes.EDITION_CONTENT,
        RecordTypes.LIBRETTO_EDITION,
        RecordTypes.THEORETICA_EDITION,
        RecordTypes.LIBRETTO_EDITION_CONTENT,
        RecordTypes.THEORETICA_EDITION_CONTENT,
        RecordTypes.INVENTORY_EDITION,
    ):
        return "printed"
    elif record_type_id in (
        RecordTypes.COLLECTION,
        RecordTypes.SOURCE,
        RecordTypes.LIBRETTO_SOURCE,
        RecordTypes.THEORETICA_SOURCE,
        RecordTypes.INVENTORY,
    ):
        return "manuscript"
    elif record_type_id == RecordTypes.COMPOSITE_VOLUME:
        return "composite"
    elif record_type_id == RecordTypes.WORK:
        return "work"
    else:
        return "unspecified"


def get_is_contents_record(record_type_id: int, parent_id: int | None) -> bool:
    # Defaults to False if it doesn't pass these tests.
    return bool(
        record_type_id
        in (
            RecordTypes.EDITION_CONTENT,
            RecordTypes.LIBRETTO_EDITION_CONTENT,
            RecordTypes.THEORETICA_EDITION_CONTENT,
        )
        or record_type_id
        in (
            RecordTypes.SOURCE,
            RecordTypes.LIBRETTO_SOURCE,
            RecordTypes.THEORETICA_SOURCE,
        )
        and parent_id is not None
    )


def get_is_collection_record(record_type_id: int, children_count: int) -> bool:
    return bool(
        record_type_id
        in (
            RecordTypes.COLLECTION,
            RecordTypes.LIBRETTO_SOURCE,
            RecordTypes.LIBRETTO_EDITION,
            RecordTypes.THEORETICA_SOURCE,
            RecordTypes.THEORETICA_EDITION,
        )
        and children_count > 0
    )


def country_code_from_siglum(siglum: str) -> str:
    # split the country code from the rest of the siglum, and return that.
    # If there was a problem splitting the siglum because it was malformed,
    # return it wholescale and keep going.
    split_sig = siglum.split("-")
    return split_sig[0] if len(split_sig) > 0 else siglum


COUNTRY_CODE_MAPPING = {
    "A": [
        "Austria",
        "L'Autriche",
        "Österreich",
    ],
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
    "CY": ["Cyprus"],
    "CZ": ["Czechoslovakia", "Czech Republic"],
    "D": [
        "Germany",
        "Deutschland",
        "Allemagne",
        "Germania",
        "Alemania",
        "Alemanha",
        "Niemcy",
    ],
    "DK": ["Denmark"],
    "DY": ["Benin"],
    "DZ": ["Algeria"],
    "E": ["Spain", "Espagne", "Spanien", "Spagna", "España", "Hiszpania", "Espanha"],
    "EC": ["Ecuador"],
    "ET": ["Egypt"],
    "EV": ["Estonia"],
    "F": ["France"],
    "FIN": ["Finland"],
    "GB": [
        "United Kingdom",
        "Great Britain",
        "Royaume-Uni",
        "Vereinigtes Königreich",
        "Regno Unito",
        "Reino Unido",
        "Zjednoczone Królestwo",
        "UK",
    ],
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
    "KSA": ["Saudi Arabia"],
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
    "US": [
        "America",
        "United States",
        "États Unis",
        "Stany Zjednoczone AP",
        "Vereinigte Staaten",
        "Amerika",
        "Estados Unidos",
        "Stati Uniti",
        "USA",
    ],
    "USB": ["Uzbekistan"],
    "UY": ["Uruguay"],
    "V": ["Vatican", "Vatikan", "Vaticano", "Watykan", "Holy See"],
    "VE": ["Venezuela"],
    "VN": ["Vietname"],
    "XX": [],
    "ZA": ["South Africa"],
}

ISO3166_TO_SIGLUM_MAPPING = {
    "XA-AD": "AND",
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
    "XA-BY": "BY",
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
    "XA-MD": "MD",
    "XA-ME": "MNE",
    "XA-MT": "M",
    "XA-NL": "NL",
    "XA-NO": "N",
    "XA-PL": "PL",
    "XA-PT": "P",
    "XA-RO": "RO",
    "XA-RS": "SRB",
    "XA-RU": "RUS",
    "XA-SA": "KSA",
    "XA-SE": "S",
    "XA-SI": "SI",
    "XA-SK": "SK",
    "XA-UA": "UA",
    "XA-VA": "V",
    "XB-AM": "ARM",
    "XB-CN": "CN",
    "XB-GE": "GE",
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
    "XC-GH": "GH",
    "XC-ZA": "ZA",
    "XD-AR": "RA",
    "XD-BO": "BOL",
    "XD-BR": "BR",
    "XD-CA": "CDN",
    "XD-CL": "RCH",
    "XD-CO": "CO",
    "XD-CR": "CR",
    "XD-CU": "C",
    "XD-EC": "EC",
    "XD-GT": "GCA",
    "XD-HN": "HN",
    "XD-JM": "JA",
    "XD-MX": "MEX",
    "XD-PE": "PE",
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


def transform_rism_id(q_id: str) -> str | None:
    """
    Transform an incoming RISM URL ID into a RISM Solr Document ID.
    :param q_id: Query ID
    :return: A Solr ID string, or None if not successful.
    """
    if not q_id:
        return None

    # doc_matcher: re.Match[str] | None = re.match(RISM_ID_SUB, q_id)
    # if not doc_matcher:
    #     return None
    doc_matcher = q_id.split("/")
    if len(doc_matcher) != 2:
        return None

    doc_type, doc_num = doc_matcher

    if doc_type == "people":  # noqa: SIM116
        return f"person_{doc_num}"
    elif doc_type == "sources":
        return f"source_{doc_num}"
    elif doc_type == "institutions":
        return f"institution_{doc_num}"
    else:
        return None
