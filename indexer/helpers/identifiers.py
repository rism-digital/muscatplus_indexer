from enum import unique, IntEnum


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
