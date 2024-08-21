import csv
import functools
import logging.config
import math
import re
from typing import Optional, Tuple, Pattern

import edtf
import yaml
from edtf.parser.edtf_exceptions import EDTFParseException

from indexer.helpers.db import mysql_pool
from indexer.helpers.marc import create_marc
from indexer.helpers.utilities import to_solr_multi

log_config: dict = yaml.full_load(open("../logging.yml", "r"))

logging.config.dictConfig(log_config)
log = logging.getLogger("date_checker")

# normalize any dates with dot divisions; used as a matcher, not a substitute.
DOT_DIVIDED_REGEX: Pattern = re.compile(
    r"(\d{2}\.)?(\d{2})\.(\d{4})(-(\d{2}\.)?(\d{2})\.(\d{4}))?"
)
CENTURY_REGEX: Pattern = re.compile(
    r"^(?P<century>\d{2})(?:th|st|rd) century, (?P<adjective1>\w+)(?: (?P<adjective2>\w+))?$",
    re.IGNORECASE,
)
# Parses dates like '18.2q' (18th century, second quarter) or '19.in' (beginning of the 19th Century)
# Also matches "20.sc" ("20eme siecle")
ANOTHER_CENTURY_REGEX: Pattern = re.compile(
    r"^(?P<century>\d{2})\.(?P<adjective1>[\diesm])(?P<adjective2>[dqhtnxce])$"
)
CENTURY_DASHES_REGEX: Pattern = re.compile(r"^(\d\d)(?:--|\?\?)$")
CENTURY_TRUNCATED_REGEX: Pattern = re.compile(r"(?P<first>\d{2})/(?P<second>\d{2})")

# Some date ranges are given as "YYYY-MM-DD-YYYY-MM-DD" so we want to swap the second hyphen for a slash.
MULTI_YEAR_REGEX: Pattern = re.compile(
    r"^(?P<first>\d{4}-\d{2}-\d{2})-(?P<second>\d{4}-\d{2}-\d{2})"
)
# A lot of dates have a letters attached to them for some odd reason.
STRIP_LETTERS: Pattern = re.compile(r"(?P<year>\d{4})(?:c|p|q|a|!])")
# Find any cases like "between XXXX and YYYY". Also handles French ('entre XXXX et YYYY') and german ('um XXXX bis um XXXX)
EXPLICIT_BETWEEN: Pattern = re.compile(
    r"^.*(?:between|entre|um|von|vor).*(?P<first>\d{4}).*(?P<second>\d{4}).*$",
    re.IGNORECASE,
)
# Any ranges with explicitly named century periods in parens can be ignored too, e.g., "1750-1799 (18.2d)"
# Also, any ones with just a single date can be ignored. We can combine these parenthetical statements into
# a single regex statement afterwards.
PARENTHETICAL_APPENDAGES1: Pattern = re.compile(r"(?P<year>\d{4}-\d{4})\s+\(.*\)")
PARENTHETICAL_APPENDAGES2: Pattern = re.compile(r"(?P<year>\d{4})\s+\(.*\)")
# Deal with years that have zeros as the day, e.g., 1999-10-00
ZERO_DAY_REGEX: Pattern = re.compile(r"(?P<year>\d{4})-\d{2}-00")
# Deal with dates that are mushed together, e.g., 19991010-19991020
MUSHED_TOGETHER_REGEX: Pattern = re.compile(r"(?P<first>\d{4})\d{4}")
MUSHED_TOGETHER_RANGE_REGEX: Pattern = re.compile(
    r"(?P<first>\d{4})\d{4}-(?P<second>\d{4})\d{4}"
)
INTRO_OUTRO_REGEX: Pattern = re.compile(
    r"(?P<first>\d{2})\.ex\s?\/\s?(?P<second>\d{2})\.in"
)

EARLY_CENTURY_END_YEAR: int = 10
LATE_CENTURY_START_YEAR: int = 90


def _parse_century_date_with_fraction(
    century_start: int, ordinal: str, period: str
) -> Optional[Tuple[int, int]]:
    """
    Parse dates of the form '16th century, second half', '15th century, last third', "18.2d" (second decade of the
    18th century), "17.3q" (third quarter of the 17th century), '19.in' (beginning of the 19th century), '18.ex'
    (end of the 18th century). "Beginning" and "End" are interpreted as the first and last decades. The 'century_start'
    should already be the start of the actual years, so for '20th century', 'century_start' should be 1900.

    Some dates are fudged a bit, so '20.sc' just means '20th century', but we accept 'c' as the period, and 's' as the
    ordinal. This might get a bit tricky if we have overlapping meanings...
    :param century_start: e.g. 1500
    :param ordinal: e.g first
    :param period: e.g. quarter
    :return: A tuple corresponding to the correct span of years.
    """
    log.debug(
        "Century start: %s, ordinal: %s, period: %s", century_start, ordinal, period
    )

    divider: int
    if period in ("half", "h"):
        divider = 2
    elif period in ("third", "t"):
        divider = 3
    elif period in ("quarter", "q"):
        divider = 4
    # interpret 'beginning' (n) and 'end' (x) as a decade, as in '18.ex' or '19.in'
    elif period in ("d", "n", "x"):
        divider = 10
    elif period in ("c", "e"):
        divider = 1
    else:
        log.debug("Unknown period %s when parsing century date", period)
        return None

    multiplier: int
    if ordinal.isdigit():
        multiplier = int(ordinal)
    # if the beginning, treat it as the first decade
    elif ordinal in ("first", "i"):
        multiplier = 1
    elif ordinal == "second":
        multiplier = 2
    elif ordinal == "third":
        multiplier = 3
    elif ordinal == "fourth":
        multiplier = 4
    # if the ending, treat it as the last decade
    elif ordinal in ("last", "e", "s", "m"):
        multiplier = divider
    else:
        log.debug("Unknown ordinal %s when parsing century date", ordinal)
        return None

    period_years: int = math.floor(100 / divider)
    return century_start + ((multiplier - 1) * period_years), century_start + (
        multiplier * period_years
    )


def _parse_century_date_with_adjective(
    century_start: int, adjective: str
) -> Optional[Tuple[int, int]]:
    """
    Parse dates of the form '16th century, early', '15th century, end'
    :param century_start: e.g. 1500
    :param adjective: e.g. early
    :return:
    """
    if adjective in ("beginning", "start", "early"):
        return century_start, century_start + EARLY_CENTURY_END_YEAR
    if adjective in ("late", "end"):
        return century_start + LATE_CENTURY_START_YEAR, century_start + 100
    if adjective == "middle":
        return century_start + 25, century_start + 75

    return None


def _format_start_end_from_century(start: int, end: int) -> tuple:
    # 18 = (18 - 1) = 17 * 100 = 1700
    first: int = (start - 1) * 100
    # 19 = 19 * 100 = 1900 - 1 = 1899
    second: int = (end * 100) - 1
    return first, second


@functools.lru_cache(maxsize=2048)
def parse_date_statement(date_statement: str) -> Tuple[Optional[int], Optional[int]]:  # noqa: MC0001
    # Optimize for non-date years; return as early as possible if we know we can't get any further information.
    if not date_statement or date_statement in (
        "[s.a.]",
        "[s. a.]",
        "s/d",
        "n/d",
        "(s.d.)",
        "[s.d.]",
        "[s.d]",
        "[s. d.]",
        "s. d.",
        "s.d.",
        "[n.d.]",
        "n. d.",
        "n.d.",
        "[n. d.]",
        "[o.J]",
        "o.J",
        "o.J.",
        "[s.n.]",
        "(s. d.)",
    ):
        return None, None

    # simplify known problems for the edtf parser
    simplified_date_statement = date_statement.replace("(?)", "")

    # Replace any dates that use dots instead of dashes to separate the parameters.
    if DOT_DIVIDED_REGEX.match(simplified_date_statement):
        simplified_date_statement = simplified_date_statement.replace(".", "-")

    simplified_date_statement = re.sub(r"[?\[\]]", r"", simplified_date_statement)
    simplified_date_statement = re.sub(
        STRIP_LETTERS, r"\g<year>", simplified_date_statement
    )
    simplified_date_statement = re.sub(
        ZERO_DAY_REGEX, r"\g<year>", simplified_date_statement
    )
    simplified_date_statement = re.sub(
        MUSHED_TOGETHER_REGEX, r"\g<first>", simplified_date_statement
    )
    simplified_date_statement = re.sub(
        MULTI_YEAR_REGEX, r"\g<first>/\g<second>", simplified_date_statement
    )
    simplified_date_statement = re.sub(
        EXPLICIT_BETWEEN, r"\g<first>/\g<second>", simplified_date_statement
    )
    simplified_date_statement = re.sub(
        MUSHED_TOGETHER_RANGE_REGEX, r"\g<first>/\g<second>", simplified_date_statement
    )
    simplified_date_statement = re.sub(
        PARENTHETICAL_APPENDAGES1, r"\g<year>", simplified_date_statement
    )
    simplified_date_statement = re.sub(
        PARENTHETICAL_APPENDAGES2, r"\g<year>", simplified_date_statement
    )
    # Any remaining parenthesis should be dropped from anywhere in the string
    simplified_date_statement = re.sub(r"([\(\)])", "", simplified_date_statement)
    # Strip any leading or trailing quotation marks.
    simplified_date_statement = simplified_date_statement.lstrip('"').rstrip('"')
    simplified_date_statement = simplified_date_statement.replace(
        "not after", "before"
    ).replace("not before", "after")
    simplified_date_statement = simplified_date_statement.strip()
    log.debug("Parsing %s simplified to %s", date_statement, simplified_date_statement)

    # handles 17-- or 17?? case
    dashes_match = CENTURY_DASHES_REGEX.match(simplified_date_statement)
    if dashes_match:
        start_century_year = int(dashes_match.group(1)) * 100
        return start_century_year, start_century_year + 99

    # Parse "18/19" (i.e., 18th-19th centuries) into (1700, 1899)
    if slashes_match := CENTURY_TRUNCATED_REGEX.match(simplified_date_statement):
        first_val: int = int(slashes_match.group("first"))
        second_val: int = int(slashes_match.group("second"))
        return _format_start_end_from_century(first_val, second_val)

    # In the case of "18.ex / 19.in", turn this into (1790 / 1810)
    if intro_outro_match := INTRO_OUTRO_REGEX.match(simplified_date_statement):
        first_val: int = int(intro_outro_match.group("first"))
        second_val: int = int(intro_outro_match.group("second"))
        first, second = _format_start_end_from_century(first_val, second_val)
        return first + 90, second - 89

    # handle integers directly
    # edtf assumes that 2 digit years are shortened years - e.g. 79 is 1979, 20 is 2020 etc. It should instead assume
    # 79 is 79 AD I only know of one example in our data, which this handles, but if we get some like 79-89 we should
    # come back to this. The problem is in the dateutil parse function
    if simplified_date_statement.isdigit():
        return int(simplified_date_statement), int(simplified_date_statement)

    # edtf doesn't support advanced century parsing - it interprets '15th century, early' as [1400-1499]
    # we try our own basic parsing for the most common cases
    century_match = CENTURY_REGEX.fullmatch(simplified_date_statement)
    # Try again with another style
    if not century_match:
        log.debug("First century did not match; trying another.")
        century_match = ANOTHER_CENTURY_REGEX.fullmatch(simplified_date_statement)

    if century_match:
        # Match the century (18), subtract 1 (17), and multiply by 100 (1700)
        century_start: int = (int(century_match.group("century")) - 1) * 100
        adjective1: str = century_match.group("adjective1")
        adjective2: Optional[str] = century_match.group("adjective2")
        if not adjective2:
            century_date = _parse_century_date_with_adjective(century_start, adjective1)
        else:
            century_date = _parse_century_date_with_fraction(
                century_start, adjective1, adjective2
            )

        if century_date:
            return century_date
    else:
        log.debug("Neither century regexes matched for %s", simplified_date_statement)

    parsed_date = None
    # First try the strictest processing
    try:
        parsed_date = edtf.parse_edtf(simplified_date_statement)
    except EDTFParseException as e:
        log.debug("Strict parsing failed; trying a looser approach")

    # If that didn't work, try a less strict 'natural language' approach
    if not parsed_date:
        try:
            parsed_date_string: Optional[str] = edtf.text_to_edtf(
                simplified_date_statement
            )
            if not parsed_date_string:
                raise Exception("Could not parse date string")
            log.debug("Edtf parsed as %s", parsed_date_string)
            parsed_date = edtf.parse_edtf(parsed_date_string)
        except EDTFParseException as e:
            log.debug(
                "Error parsing date %s, simplified to %s: %s",
                date_statement,
                simplified_date_statement,
                e,
            )
            raise
        except TypeError as e:
            log.debug(
                "Error parsing date %s, simplified to %s: %s",
                date_statement,
                simplified_date_statement,
                e,
            )
            raise
        except ValueError as e:
            log.debug(
                "Error parsing date %s, simplified to %s: %s",
                date_statement,
                simplified_date_statement,
                e,
            )
            raise

    # get the year for each edtf struct directly
    # We could parse as datetime instead but it's an extra step and doesn't support all the dates edtf does
    try:
        start_year: Optional[int] = parsed_date.lower_strict()[0]
        end_year: Optional[int] = parsed_date.upper_strict()[0]
    except AttributeError as e:
        # todo: remove this once https://github.com/ixc/python-edtf/issues/32 is fixed
        log.debug("Unexpected error %s while parsing %s", e, date_statement)
        raise

    # remember start_year and end_year could be 0, which is also falsey
    if start_year is not None and end_year is not None and start_year > end_year:
        log.warning(
            "Error parsing date: start %s is greater than end %s from %s, simplified to %s",
            start_year,
            end_year,
            date_statement,
            simplified_date_statement,
        )
        raise

    # edtf returns 0 and 9999 in some cases if only the year is unknown - it's pretty useless for us
    if end_year == 9999:
        end_year = None
        if start_year == 0:
            start_year = None

    if isinstance(parsed_date, edtf.Interval):
        # if one end of a date range is unknown the default is to set the strict date to 10 years before/after the
        # known date we detect that case here and make the date None instead
        # we could also consider changing edtf.appsettings.DELTA_IF_UNKNOWN
        if str(parsed_date.lower) == "unknown":
            start_year = None
        if str(parsed_date.upper) == "unknown":
            end_year = None

    return start_year, end_year


if __name__ == "__main__":
    idx_config: dict = yaml.full_load(open("../index_config.yml", "r"))

    query = """
    SELECT child.id AS id, child.marc_source AS marc_source
    FROM muscat_development.sources AS child
    WHERE child.wf_stage = 1
    ORDER BY child.id desc;
    """

    conn = mysql_pool.connection()
    curs = conn.cursor()

    curs.execute(query)

    errors: list[dict] = []

    while record := curs._cursor.fetchone():
        marc_source = record["marc_source"]
        marc_record = create_marc(marc_source)
        date_statements: Optional[list] = to_solr_multi(marc_record, "260", "c")

        if not date_statements:
            continue

        for statement in date_statements:
            try:
                earliest, latest = parse_date_statement(statement)
            except Exception as e:
                log.info("Failed parsing %s for %s", statement, record["id"])

                errors.append(
                    {
                        "source": record["id"],
                        "url": f"https://muscat.rism.info/admin/sources/{record['id']}",
                        "statement": statement,
                    }
                )

    with open("date-errors.csv", "w", newline="") as csvfile:
        fieldnames = ["source", "url", "statement"]
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(errors)
