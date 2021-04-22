RECORD_TYPES = {
    "unspecified": 0,
    "collection": 1,
    "source": 2,
    "edition_content": 3,
    "libretto_source": 4,
    "libretto_edition": 5,
    "theoretica_source": 6,
    "theoretica_edition": 7,
    "edition": 8,
    "libretto_edition_content": 9,
    "theoretica_edition_content": 10,
    "composite_volume": 11,
}

# Invert record types table for lookups
RECORD_TYPES_BY_ID = {v: k for k, v in RECORD_TYPES.items()}


def country_code_from_siglum(siglum: str) -> str:
    # split the country code from the rest of the siglum, and return that. If there was a problem splitting the siglum
    # because it was malformed, return it wholescale and keep going.
    try:
        country, _ = siglum.split("-")
    except ValueError:
        return siglum

    return country


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
