
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
