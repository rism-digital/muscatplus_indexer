import datetime
import functools
import logging.config
import math
import re

import edtf
from edtf.parser.edtf_exceptions import EDTFParseException

log = logging.getLogger("muscat_indexer")

DateRange = tuple[int | None, int | None]

# The simplest single year match
SIMPLE_SINGLE_YEAR_REGEX: re.Pattern = re.compile(r"^(?P<year>\d{4})$")
# The simplest date range -- 1234-1256
SIMPLE_RANGE_REGEX: re.Pattern = re.compile(r"^(?P<first>\d{4})-(?P<second>\d{4})$")

# dd/mm/yyyy
SLASH_DIVIDED_REGEX: re.Pattern = re.compile(
    r"^(?P<day>\d{2})/(?P<month>\d{2})/(?P<year>\d{4})"
)

# Matches NNNNNN/NN
ALT_DIVIDED_REGEX: re.Pattern = re.compile(
    r"^(?P<year>\d{4})(?P<month>\d{2})/(?P<day>\d{2})"
)

# Matches NNNN---- or NNNNNN--
ALT_DASHED_REGEX: re.Pattern = re.compile(r"^(?P<year>\d{4})(?P<month>-{2}|\d{2})--")

# normalize any dates with dot divisions; used as a matcher, not a substitute.
DOT_DIVIDED_REGEX: re.Pattern = re.compile(
    r"(\d{1,2}\.)?(\d{1,2})\.(\d{4})(-(\d{1,2}\.)?(\d{1,2})\.(\d{4}))?"
)
CENTURY_REGEX: re.Pattern = re.compile(
    r"^(?P<century>\d{2})(?:th|st|rd) century, (?P<adjective1>\w+)(?: (?P<adjective2>\w+))?$",
    re.IGNORECASE,
)
# Parses dates like '18.2q' (18th century, second quarter) or '19.in' (beginning of the 19th Century)
# Also matches "20.sc" ("20eme siecle")
ANOTHER_CENTURY_REGEX: re.Pattern = re.compile(
    r"^(?P<century>\d{2})\.(?P<adjective1>[\diesm])(?P<adjective2>[dqhtnxce])?$"
)
CENTURY_DASHES_REGEX: re.Pattern = re.compile(r"^(\d\d)(?:--|\?\?)$")
CENTURY_TRUNCATED_REGEX: re.Pattern = re.compile(r"(?P<first>\d{2})/(?P<second>\d{2})")

# Some date ranges are given as "YYYY-MM-DD-YYYY-MM-DD". We only want the years, though.
MULTI_YEAR_REGEX: re.Pattern = re.compile(
    r"^(?P<first>\d{4})-\d{2}-\d{2}-(?P<second>\d{4})-\d{2}-\d{2}"
)
# A lot of dates have a letters attached to them for some odd reason.
STRIP_LETTERS: re.Pattern = re.compile(r"(?P<year>\d{3,4})[cpqa!]")
# Find any cases like "between XXXX and YYYY". Also handles French ('entre XXXX et YYYY') and german ('um XXXX bis um XXXX)
EXPLICIT_BETWEEN: re.Pattern = re.compile(
    r"^.*(?:between|entre|um|von|vor|et).*(?P<first>\d{4}).*(?P<second>\d{4}).*$",
    re.IGNORECASE,
)
# Any ranges with explicitly named century periods in parens can be ignored too, e.g., "1750-1799 (18.2d)"
# Also, any ones with just a single date can be ignored. We can combine these parenthetical statements into
# a single regex statement afterwards.
PARENTHETICAL_APPENDAGES1: re.Pattern = re.compile(r"(?P<year>\d{4}-\d{4})\s+\(.*\)")
PARENTHETICAL_APPENDAGES2: re.Pattern = re.compile(r"(?P<year>\d{4})\s+\(.*\)")
# Deal with years that have zeros or Xs as the day, e.g., 1999-10-00, 1999-10-XX
ZERO_DAY_REGEX: re.Pattern = re.compile(r"^(?P<year>\d{4})-\d{2}-(00|XX)$")
# Deal with dates that are mushed together, e.g., 19991010-19991020
MUSHED_TOGETHER_REGEX: re.Pattern = re.compile(r"(?P<first>\d{4})\d{4}")
MUSHED_TOGETHER_RANGE_REGEX: re.Pattern = re.compile(
    r"(?P<first>\d{4})\d{4}-(?P<second>\d{4})\d{4}"
)

EARLY_CENTURY_END_YEAR: int = 10
LATE_CENTURY_START_YEAR: int = 90

NO_DATES = {
    None,
    "",
    "[s.a.]",
    "[s. a.]",
    "s.a.",
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
    "[s.l.]",
    "[s.a]",
    "xxxx-xxxx",
    "uuuu-uuuu",
    "?",
    "??",
    "[s..d]",
    "s/f",
    "[s.d. ]",
    "[s,d,]",
    "[s.t.]",
    "[o. J.]",
    "s.d",
    "[s.d.}",
    "o.d.",
    "s.t.",
    "[o.J.]",
    "(n.d.)",
    "[without]",
    "[s .a.]",
    "[s/d/]",
    "[s.d.[",
    "[s.c.]",
    "s/ d",
    "[?]",
    "[s,d.]",
    "[sd]",
    "(s.d)",
    "unk",
    "unknown",
    "[s. f.]",
    "[s. n.]",
    "[s. d,]",
    "[sine anno]",
    "XVI-XVIII",
    "XVII-XIX",
    "[20th c.]",
    "XIX-XX",
    "Año X",
}

# Each tuple is (pattern, replacement)
SIMPLIFICATION_RULES: list[tuple[re.Pattern, str]] = [
    # Strip uncertainty markers and brackets early
    (STRIP_LETTERS, r"\g<year>"),
    (ZERO_DAY_REGEX, r"\g<year>"),
    (MUSHED_TOGETHER_RANGE_REGEX, r"\g<first>/\g<second>"),
    (MULTI_YEAR_REGEX, r"\g<first>/\g<second>"),
    (EXPLICIT_BETWEEN, r"\g<first>/\g<second>"),
    (MUSHED_TOGETHER_REGEX, r"\g<first>"),
    (PARENTHETICAL_APPENDAGES1, r"\g<year>"),
    (PARENTHETICAL_APPENDAGES2, r"\g<year>"),
]


def simplify_date_statement(date_statement: str) -> str:
    """
    Normalize a raw date string into something EDTF can reasonably parse.
    This function is intentionally conservative and order-sensitive.
    """

    s: str = date_statement

    # Normalize common oddities
    s = s.replace("(?)", "?")

    # Convert dot-separated dates (dd.mm.yyyy) to dash-separated
    if DOT_DIVIDED_REGEX.match(s):
        s = s.replace(".", "-")

    # Remove uncertainty markers and brackets globally
    s = re.sub(r"[?\[\]]", "", s)

    # Apply ordered simplification rules
    for pattern, replacement in SIMPLIFICATION_RULES:
        s = pattern.sub(replacement, s)

    # Drop any remaining parentheses anywhere
    s = re.sub(r"[()]", "", s)

    # Normalize quotes and whitespace
    s = s.strip().lstrip('"').rstrip('"')

    # Normalize semantic phrases EDTF understands
    s = s.replace("not after", "before").replace("not before", "after").strip()

    return s


@functools.lru_cache(maxsize=1024)
def _parse_century_date_with_fraction(
    century_start: int, ordinal: str, period: str
) -> tuple[int, int] | None:
    """
    Parse dates of the form '16th century, second half', '15th century, last third', "18.2d" (second decade of the
    18th century), "17.3q" (third quarter of the 17th century), '19.in' (beginning of the 19th century), '18.ex'
    (end of the 18th century). "Beginning" and "End" are interpreted as the first and last decades. The 'century_start'
    should already be the start of the actual years, so for '20th century', 'century_start' should be 1900.

    Some dates are fudged a bit, so '20.sc' just means '20th century', but we accept 'c' as the period, and 's' as the
    ordinal. This might get a bit tricky if we have overlapping meanings...
    :param century_start: e.g. 1500
    :param ordinal: e.g. first
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


@functools.lru_cache(maxsize=1024)
def _parse_century_date_with_adjective(
    century_start: int, adjective: str
) -> tuple[int, int] | None:
    """
    Parse dates of the form '16th century, early', '15th century, end'
    :param century_start: e.g. 1500
    :param adjective: e.g. early
    :return:
    """
    if adjective in ("beginning", "start", "early"):
        return century_start, century_start + EARLY_CENTURY_END_YEAR
    if adjective in ("late", "end"):
        return century_start + LATE_CENTURY_START_YEAR, century_start + 99
    if adjective == "middle":
        return century_start + 25, century_start + 75

    return None


@functools.lru_cache(maxsize=2048)
def parse_date_statement(date_statement: str) -> DateRange:  # noqa: MC0001
    # Optimize for non-date years; return as early as possible if we know we can't get any further information.
    if date_statement in NO_DATES:
        return None, None

    # Skip Año and any roman numerals:
    if date_statement.startswith(("A", "M", "X")):
        return None, None

    if date_statement.startswith("-"):
        log.warning("Stripping leading hyphen off date: %s", date_statement)
        date_statement = date_statement[1:]

    # Fast path: If we have a single date of four digits, don't bother doing any additional processing.
    if simplest_single_match := SIMPLE_SINGLE_YEAR_REGEX.match(date_statement):
        year: int = int(simplest_single_match.group("year"))
        return year, year

    first: int
    second: int
    # Fast path: If we have a really simple range, then short circuit all additional processing
    # and check this first.
    if simplest_range_match := SIMPLE_RANGE_REGEX.match(date_statement):
        first = int(simplest_range_match.group("first"))
        second = int(simplest_range_match.group("second"))

        return first, second

    if simplest_slash_match := SLASH_DIVIDED_REGEX.match(date_statement):
        year = int(simplest_slash_match.group("year"))

        return year, year

    if alt_slash_match := ALT_DIVIDED_REGEX.match(date_statement):
        year = int(alt_slash_match.group("year"))

        return year, year

    if alt_dash_match := ALT_DASHED_REGEX.match(date_statement):
        year = int(alt_dash_match.group("year"))

        return year, year

    simplified_date_statement = simplify_date_statement(date_statement)
    log.debug(
        "Parsing %s simplified to %s",
        date_statement,
        simplified_date_statement,
    )

    # adds / subtracts 99 years if a person's birth or death dates are the only known dates
    if simplified_date_statement.endswith("*") or simplified_date_statement.endswith(
        "+"
    ):
        year_section: str = simplified_date_statement[:4]
        if year_section.isdigit():
            if simplified_date_statement.endswith("*"):
                return int(year_section), int(year_section) + 99
            elif simplified_date_statement.endswith("+"):
                return int(year_section) - 99, int(year_section)

    # handles 17-- or 17?? case
    dashes_match = CENTURY_DASHES_REGEX.match(simplified_date_statement)
    if dashes_match:
        start_century_year = int(dashes_match.group(1)) * 100
        return start_century_year, start_century_year + 99

    # Parse "18/19" (i.e., 18th-19th centuries) into (1700, 1899)
    if slashes_match := CENTURY_TRUNCATED_REGEX.match(simplified_date_statement):
        # 18 = 17 * 100 = 1700
        first = (int(slashes_match.group("first")) - 1) * 100
        # 19 = 18 * 100 = 1800 + 50 = 1850
        second = ((int(slashes_match.group("second"))) * 100) - 1
        return first, second

    # handle cleaned integers directly
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
        adjective2: str | None = century_match.group("adjective2")
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
    except EDTFParseException:
        log.debug("Strict parsing failed; trying a looser approach")

    # If that didn't work, try a less strict 'natural language' approach
    if not parsed_date:
        try:
            parsed_date_string: str | None = edtf.text_to_edtf(
                simplified_date_statement
            )
            if not parsed_date_string:
                log.debug(
                    "Edtf parsing failed for %s, simplified to %s",
                    date_statement,
                    simplified_date_statement,
                )
                return None, None
            log.debug("Edtf parsed as %s", parsed_date_string)
            parsed_date = edtf.parse_edtf(parsed_date_string)
        except EDTFParseException as e:
            log.debug(
                "Error parsing date %s, simplified to %s: %s",
                date_statement,
                simplified_date_statement,
                e,
            )
            return None, None
        except TypeError as e:
            log.debug(
                "Error parsing date %s, simplified to %s: %s",
                date_statement,
                simplified_date_statement,
                e,
            )
            return None, None
        except ValueError as e:
            log.debug(
                "Error parsing date %s, simplified to %s: %s",
                date_statement,
                simplified_date_statement,
                e,
            )
            return None, None

    # get the year for each edtf struct directly
    # We could parse as datetime instead but it's an extra step and doesn't support all the dates edtf does
    try:
        start_year: int | None = parsed_date.lower_strict()[0]
        end_year: int | None = parsed_date.upper_strict()[0]
    except AttributeError as e:
        log.debug("Unexpected error %s while parsing %s", e, date_statement)
        return None, None

    # remember start_year and end_year could be 0, which is also falsey
    if start_year is not None and end_year is not None and start_year > end_year:
        log.warning(
            "Error parsing date: start %s is greater than end %s from %s, simplified to %s",
            start_year,
            end_year,
            date_statement,
            simplified_date_statement,
        )
        return None, None

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


EARLIEST_YEAR_IF_MISSING: int = -2000
LATEST_YEAR_IF_MISSING: int = datetime.datetime.now().year


def process_date_statements(
    date_statements: list[str], record_id: str
) -> list[int] | None:
    earliest_dates: list[int] = []
    latest_dates: list[int] = []

    for statement in date_statements:
        if statement in NO_DATES:
            continue

        # Skip Año and any roman numerals:
        if statement.startswith(("A", "M", "X")):
            return None

        if statement.startswith("-"):
            log.warning(
                "A date statement with a leading hyphen was detected: %s, record ID %s.",
                statement,
                record_id,
            )

        if "\u200f" in statement:
            log.warning(
                "A right-to-left unicode character was detected in %s, record %s",
                statement,
                record_id,
            )

        try:
            earliest, latest = parse_date_statement(statement)
        except Exception as e:  # noqa
            # The breadth of errors mean we could spend all day catching things, so in this case we use
            # a blanket exception catch and then log the statement to be fixed so that we might fix it later.
            log.warning("Error parsing date statement %s: %s", statement, e)
            return None

        if earliest is None and latest is None:
            log.warning(
                "Problem with date statement %s for record %s", statement, record_id
            )
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
    earliest_date: int = (
        max(min(earliest_dates), EARLIEST_YEAR_IF_MISSING)
        if earliest_dates
        else EARLIEST_YEAR_IF_MISSING
    )
    latest_date: int = (
        min(max(latest_dates), LATEST_YEAR_IF_MISSING)
        if latest_dates
        else LATEST_YEAR_IF_MISSING
    )

    # If neither date was parseable, don't pretend we have a date.
    if (
        earliest_date <= EARLIEST_YEAR_IF_MISSING
        and latest_date >= LATEST_YEAR_IF_MISSING
    ):
        return None

    if earliest_date > latest_date:
        log.warning(
            "Earliest date %s is greater than latest date %s for record %s",
            earliest_date,
            latest_date,
            record_id,
        )
        return None

    # If we have a case where the earliest date is in the negatives
    # and the latest date is more than 300 CE, then assume a problem
    # and set the earliest and latest date to the same value.
    if earliest_date < 0 and latest_date > 300:
        log.warning(
            "The earliest date %s is unlikely. Setting it to the same value as the latest date, %s. record: %s",
            earliest_date,
            latest_date,
            record_id,
        )
        earliest_date = latest_date

    # If we don't have one, but we do have the other, these will still be valid (but
    # improbable) integer values.
    return [earliest_date, latest_date]


def convert_to_edtf(old_date: str) -> str:
    if len(old_date) == 5:
        if old_date.endswith("c"):
            return f"{old_date[0:4]}~"
        elif old_date.endswith("p"):
            return f"{old_date[0:4]}/.."
        elif old_date.endswith("a"):
            return f"../{old_date[0:4]}"
        else:
            return old_date
    elif len(old_date) > 5:
        if old_date.endswith(".0"):
            return f"{old_date[0:4]}"
        elif old_date.startswith("ca. ") or old_date.startswith("um"):
            return f"{old_date[-4:]}~"
        elif old_date.startswith("nach"):
            return f"{old_date[-4:]}/.."
        else:
            return old_date
    else:
        return old_date
