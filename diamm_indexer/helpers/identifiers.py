# Relates source relationship table FKs to MARC relator codes.
RELATOR_MAP: dict = {
    "1": "dte",
    "2": "dto",
    "3": "edt",
    "4": "asn",  # Owner -> associated name
    "5": "pbl",
    "6": "scr",  # "scriptorium" -> "scribe"
    "7": "bnd",
    "8": "pat",
    "9": "asn",  # "reference to person in text" -> "associated name"
    "10": "aut",
    "11": "com",
    "12": "asn",  # decoration style -> associated name
    "13": "asn",  # "mentioned in dedication" -> "associated name"
    "14": "asn",  # "discovered by" -> "associated name"
    "15": "pat",  # "commissioned" -> "patron"
    "16": "pat",
    "18": "evp",  # "copied at" -> "manufacture place"
    "23": "asn",  # "described ms" -> "expert"
    "24": "asn",
    "25": "asn",
    "26": "asn",
    "27": "asn",
    "28": "trl",
    "29": "oth",
    "30": "oth",
    "31": "oth",
    "32": "pat",
    "35": "dte",
    "37": "aut",
    "45": "oth",
    "46": "pbl",
}


# Maps the geographic area FK for entries marked as "country" to RISM siglum prefixes
COUNTRY_SIGLUM_MAP: dict = {
    "1": "A",  # Austria
    "2": "AUS",  # Australia
    "3": "B",  # Belgium
    "4": "CH",  # Switzerland
    "5": "D",  # Germany
    "6": "E",  # Spain
    "7": "EV",  # Estonia
    "8": "F",  # France
    "9": "I",  # Italy
    "10": "GB",  # United Kingdom
    "11": "DK",  # Denmark
    "12": "CZ",  # Czech Republic
    "13": "IRL",  # Ireland
    "14": "NL",  # Netherlands
    "15": "PL",  # Poland
    "16": "RUS",  # Russian Federation
    "17": "S",  # Sweden
    "18": "US",  # United States
    "19": "HR",  # Croatia
    "20": "H",  # Hungary
    "21": "P",  # Portugal
    "22": "MEX",  # Mexico
    "23": "IS",  # Iceland
    "24": "ZA",  # South Africa
    "25": "GCA",  # Guatemala
    "26": "CO",  # Colombia
    "27": "SI",  # Slovenia
    "28": "SK",  # Slovakia
    "29": "GB",  # Northern Ireland
    "433": "GB",  # England
    "435": "GB",  # Scotland
    "436": "J",  # Japan
    "686": "CY",  # Cyprus
    "752": "GB",  # Wales
    "810": "UA",  # Ukraine
    "816": "PE",  # Peru
}
