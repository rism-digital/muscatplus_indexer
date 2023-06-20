import datetime
import functools
import logging.config
import math
import re
from typing import Pattern, Optional

import edtf
from edtf.parser.edtf_exceptions import EDTFParseException

log = logging.getLogger("muscat_indexer")
# assume European format dates
edtf.appsettings.DAY_FIRST = True

# The simplest single year match
SIMPLE_SINGLE_YEAR_REGEX: Pattern = re.compile("^(?P<year>\d{4})$")
# The simplest date range -- 1234-1256
SIMPLE_RANGE_REGEX: Pattern = re.compile("^(?P<first>\d{4})-(?P<second>\d{4})$")

# normalize any dates with dot divisions; used as a matcher, not a substitute.
DOT_DIVIDED_REGEX: Pattern = re.compile(r"(\d{2}\.)?(\d{2})\.(\d{4})(-(\d{2}\.)?(\d{2})\.(\d{4}))?")
CENTURY_REGEX: Pattern = re.compile(r'^(?P<century>\d{2})(?:th|st|rd) century, (?P<adjective1>\w+)(?: (?P<adjective2>\w+))?$', re.IGNORECASE)
# Parses dates like '18.2q' (18th century, second quarter) or '19.in' (beginning of the 19th Century)
# Also matches "20.sc" ("20eme siecle")
ANOTHER_CENTURY_REGEX: Pattern = re.compile(r'^(?P<century>\d{2})\.(?P<adjective1>[\diesm])(?P<adjective2>[dqhtnxce])$')
CENTURY_DASHES_REGEX: Pattern = re.compile(r'^(\d\d)(?:--|\?\?)$')
CENTURY_TRUNCATED_REGEX: Pattern = re.compile(r"(?P<first>\d{2})/(?P<second>\d{2})")

# Some date ranges are given as "YYYY-MM-DD-YYYY-MM-DD" so we want to swap the second hyphen for a slash.
MULTI_YEAR_REGEX: Pattern = re.compile(r'^(?P<first>\d{4}-\d{2}-\d{2})-(?P<second>\d{4}-\d{2}-\d{2})')
# A lot of dates have a letters attached to them for some odd reason.
STRIP_LETTERS: Pattern = re.compile(r"(?P<year>\d{3,4})(?:c|p|q|a|!])")
# Find any cases like "between XXXX and YYYY". Also handles French ('entre XXXX et YYYY') and german ('um XXXX bis um XXXX)
EXPLICIT_BETWEEN: Pattern = re.compile(r"^.*(?:between|entre|um|von|vor|et).*(?P<first>\d{4}).*(?P<second>\d{4}).*$", re.IGNORECASE)
# Any ranges with explicitly named century periods in parens can be ignored too, e.g., "1750-1799 (18.2d)"
# Also, any ones with just a single date can be ignored. We can combine these parenthetical statements into
# a single regex statement afterwards.
PARENTHETICAL_APPENDAGES1: Pattern = re.compile(r"(?P<year>\d{4}-\d{4})\s+\(.*\)")
PARENTHETICAL_APPENDAGES2: Pattern = re.compile(r"(?P<year>\d{4})\s+\(.*\)")
# Deal with years that have zeros as the day, e.g., 1999-10-00
ZERO_DAY_REGEX: Pattern = re.compile(r"(?P<year>\d{4})-\d{2}-00")
# Deal with dates that are mushed together, e.g., 19991010-19991020
MUSHED_TOGETHER_REGEX: Pattern = re.compile(r"(?P<first>\d{4})\d{4}")
MUSHED_TOGETHER_RANGE_REGEX: Pattern = re.compile(r"(?P<first>\d{4})\d{4}-(?P<second>\d{4})\d{4}")

EARLY_CENTURY_END_YEAR: int = 10
LATE_CENTURY_START_YEAR: int = 90


def _parse_century_date_with_fraction(century_start: int, ordinal: str, period: str) -> Optional[tuple[int, int]]:
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
    log.debug("Century start: %s, ordinal: %s, period: %s", century_start, ordinal, period)

    divider: int
    if period in ('half', 'h'):
        divider = 2
    elif period in ('third', 't'):
        divider = 3
    elif period in ('quarter', 'q'):
        divider = 4
    # interpret 'beginning' (n) and 'end' (x) as a decade, as in '18.ex' or '19.in'
    elif period in ('d', 'n', 'x'):
        divider = 10
    elif period in ('c', 'e'):
        divider = 1
    else:
        log.debug('Unknown period %s when parsing century date', period)
        return None

    multiplier: int
    if ordinal.isdigit():
        multiplier = int(ordinal)
    # if the beginning, treat it as the first decade
    elif ordinal in ('first', 'i'):
        multiplier = 1
    elif ordinal == 'second':
        multiplier = 2
    elif ordinal == 'third':
        multiplier = 3
    elif ordinal == 'fourth':
        multiplier = 4
    # if the ending, treat it as the last decade
    elif ordinal in ('last', 'e', 's', 'm'):
        multiplier = divider
    else:
        log.debug('Unknown ordinal %s when parsing century date', ordinal)
        return None

    period_years: int = math.floor(100 / divider)
    return century_start + ((multiplier - 1) * period_years), century_start + (multiplier * period_years)


def _parse_century_date_with_adjective(century_start: int, adjective: str) -> Optional[tuple[int, int]]:
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


@functools.lru_cache(maxsize=2048)
def parse_date_statement(date_statement: str) -> tuple[Optional[int], Optional[int]]:  # noqa: MC0001
    # Optimize for non-date years; return as early as possible if we know we can't get any further information.
    if not date_statement or date_statement in ("[s.a.]", "[s. a.]", "[s.d.]", "[s. d.]", "s. d.", "s.d.", "[n.d.]",
                                                "[o.J]", "o.J", "[s.n.]", "(s. d.)", "[s.l.]", "[s.a]"):
        return None, None

    if "\u200f" in date_statement:
        log.warning("A right-to-left unicode character was detected in %s", date_statement)

    # Fast path: If we have a single date of four digits, don't bother doing any additional processing.
    if simplest_single_match := SIMPLE_SINGLE_YEAR_REGEX.match(date_statement):
        year: int = int(simplest_single_match.group("year"))
        return year, year

    # Fast path: If we have a really simple range, then short circuit all additional processing
    # and check this first.
    if simplest_range_match := SIMPLE_RANGE_REGEX.match(date_statement):
        first: int = int(simplest_range_match.group("first"))
        second: int = int(simplest_range_match.group("second"))

        return first, second

    # Slow path
    # First simplify known problems for the edtf parser
    simplified_date_statement = date_statement.replace('(?)', '')

    # Replace any dates that use dots instead of dashes to separate the parameters.
    if DOT_DIVIDED_REGEX.match(simplified_date_statement):
        simplified_date_statement = simplified_date_statement.replace(".", "-")

    simplified_date_statement = re.sub(r'[?\[\]]', r'', simplified_date_statement)
    simplified_date_statement = re.sub(STRIP_LETTERS, r"\g<year>", simplified_date_statement)
    simplified_date_statement = re.sub(ZERO_DAY_REGEX, r"\g<year>", simplified_date_statement)
    simplified_date_statement = re.sub(MUSHED_TOGETHER_REGEX, r"\g<first>", simplified_date_statement)
    simplified_date_statement = re.sub(MULTI_YEAR_REGEX, r"\g<first>/\g<second>", simplified_date_statement)
    simplified_date_statement = re.sub(EXPLICIT_BETWEEN, r'\g<first>/\g<second>', simplified_date_statement)
    simplified_date_statement = re.sub(MUSHED_TOGETHER_RANGE_REGEX, r'\g<first>/\g<second>', simplified_date_statement)
    simplified_date_statement = re.sub(PARENTHETICAL_APPENDAGES1, r"\g<year>", simplified_date_statement)
    simplified_date_statement = re.sub(PARENTHETICAL_APPENDAGES2, r"\g<year>", simplified_date_statement)
    # Any remaining parenthesis should be dropped from anywhere in the string
    simplified_date_statement = re.sub(r"([\(\)])", "", simplified_date_statement)
    # Strip any leading or trailing quotation marks.
    simplified_date_statement = simplified_date_statement.lstrip("\"").rstrip("\"")
    simplified_date_statement = simplified_date_statement.replace('not after', 'before').replace('not before', 'after')
    simplified_date_statement = simplified_date_statement.strip()
    log.debug("Parsing %s simplified to %s", date_statement, simplified_date_statement)

    # adds / subtracts 99 years if a person's birth or death dates are the only known dates
    if simplified_date_statement.endswith("*") or simplified_date_statement.endswith("+"):
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
        first: int = ((int(slashes_match.group("first")) - 1) * 100)
        # 19 = 18 * 100 = 1800 + 50 = 1850
        second: int = ((int(slashes_match.group("second"))) * 100) - 1
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
        adjective2: Optional[str] = century_match.group("adjective2")
        if not adjective2:
            century_date = _parse_century_date_with_adjective(century_start, adjective1)
        else:
            century_date = _parse_century_date_with_fraction(century_start, adjective1, adjective2)

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
            parsed_date_string: Optional[str] = edtf.text_to_edtf(simplified_date_statement)
            if not parsed_date_string:
                return None, None
            log.debug("Edtf parsed as %s", parsed_date_string)
            parsed_date = edtf.parse_edtf(parsed_date_string)
        except EDTFParseException as e:
            log.debug("Error parsing date %s, simplified to %s: %s", date_statement, simplified_date_statement, e)
            return None, None
        except TypeError as e:
            log.debug("Error parsing date %s, simplified to %s: %s", date_statement, simplified_date_statement, e)
            return None, None
        except ValueError as e:
            log.debug("Error parsing date %s, simplified to %s: %s", date_statement, simplified_date_statement, e)
            return None, None

    # get the year for each edtf struct directly
    # We could parse as datetime instead but it's an extra step and doesn't support all the dates edtf does
    try:
        start_year: Optional[int] = parsed_date.lower_strict()[0]
        end_year: Optional[int] = parsed_date.upper_strict()[0]
    except AttributeError as e:
        # todo: remove this once https://github.com/ixc/python-edtf/issues/32 is fixed
        log.debug("Unexpected error %s while parsing %s", e, date_statement)
        return None, None

    # remember start_year and end_year could be 0, which is also falsey
    if start_year is not None and end_year is not None and start_year > end_year:
        log.warning('Error parsing date: start %s is greater than end %s from %s, simplified to %s',
                    start_year, end_year, date_statement, simplified_date_statement)
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
        if str(parsed_date.lower) == 'unknown':
            start_year = None
        if str(parsed_date.upper) == 'unknown':
            end_year = None

    return start_year, end_year


def parse_date_metadata(date_statements: list[str], start_year: Optional[int],
                        end_year: Optional[int]) -> tuple[Optional[int], Optional[int]]:
    """
    Parse the date metadata we're given in mods/dc (usually some combination of date statements, start year and end year)
    into start year and end year, with missing information filled in from the other date fields where possible
    We need the start and end date information to judge if we should be parsing from date statements and to check ordering
    If non-null, start year will always be <= end year
    :param date_statements:
    :param start_year:
    :param end_year:
    :return: (start_year, end_year, date_statements)
    """
    # we only try to parse start and end from the date statement if we have neither - otherwise we end up with
    # inconsistencies
    if not start_year and not end_year:
        # parse from date statement
        if not date_statements:
            return None, None

        if len(date_statements) == 1:
            # parse the start and end from the date statment if possible
            start_year, end_year = parse_date_statement(date_statements[0])
        else:
            # We don't know which of the date statements is the start or end, and sometimes they're ranges
            # so we just take the earliest start and latest end
            date_ranges_from_date_statements = [parse_date_statement(d) for d in date_statements]
            start_years = [start for start, _ in date_ranges_from_date_statements if start is not None]
            end_years = [end for _, end in date_ranges_from_date_statements if end is not None]
            if start_years:
                start_year = min(start_years)
            if end_years:
                end_year = max(end_years)

    if start_year is not None and end_year is not None and start_year > end_year:
        log.warning('Error parsing date: start %s is greater than end %s', start_year, end_year)
        return None, None

    return start_year, end_year


EARLIEST_YEAR_IF_MISSING: int = -2000
LATEST_YEAR_IF_MISSING: int = datetime.datetime.now().year


def process_date_statements(date_statements: list[str], record_id: str) -> Optional[list[int]]:
    earliest_dates: list[int] = []
    latest_dates: list[int] = []

    for statement in date_statements:
        if not statement or statement in {"[s.a.]", "[s. a.]", "s/d", "n/d", "(s.d.)", "[s.d.]", "[s.d]", "[s. d.]",
                                          "s. d.", "s.d.", "[n.d.]", "n. d.", "n.d.", "[n. d.]", "[o.J]", "o.J",
                                          "o.J.", "[s.n.]", "(s. d.)", "[s.l.]", "[s.a]", "xxxx-xxxx", "uuuu-uuuu",
                                          "?", "??"}:
            continue

        try:
            earliest, latest = parse_date_statement(statement)
        except Exception as e:  # noqa
            # The breadth of errors mean we could spend all day catching things, so in this case we use
            # a blanket exception catch and then log the statement to be fixed so that we might fix it later.
            log.warning("Error parsing date statement %s: %s", statement, e)
            return None

        if earliest is None and latest is None:
            log.warning("Problem with date statement %s for record %s", statement, record_id)
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
