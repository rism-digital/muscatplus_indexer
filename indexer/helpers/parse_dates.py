"""Date parsing helpers for the RISM MuscatPlus indexer.

Thin adapter around the ``antequem`` EDTF parser. Bridges the
application-specific year-range heuristics (±200-year fallbacks,
``NO_DATES`` filtering, etc.) with the ``antequem`` parser, natlang
coercion, and public year-bounds API.
"""

import datetime
import logging
import re

import antequem
from antequem.natlang import coerce as natlang_coerce

log = logging.getLogger("muscat_indexer")

DateRange = tuple[int | None, int | None]

_EARLIEST_YEAR_FALLBACK: int = -2000
_LATEST_YEAR_FALLBACK: int = datetime.datetime.now().year
_DEFAULT_GAP: int = 50
_DECADE_GAP: int = 10
_OPEN_RIGHT_SUFFIX_RANGE_RE = re.compile(
    r"^(?P<first>\d{4})(?P<first_mark>[ac]?)-(?P<second>\d{4})p$",
    re.IGNORECASE,
)
_OPEN_LEFT_SUFFIX_RANGE_RE = re.compile(
    r"^(?P<first>\d{4})p-(?P<second>\d{4})(?P<second_mark>[acp]?)$",
    re.IGNORECASE,
)
_OPEN_RIGHT_LIFE_MARKER_RE = re.compile(
    r"^(?P<year>\d{4})p(?P<life>[*+])$",
    re.IGNORECASE,
)


def simplify_date_statement(date_statement: str) -> str:
    """Normalize a raw date string into something parseable."""
    coerced = natlang_coerce(date_statement)
    if coerced is None:
        return date_statement
    return coerced


# ------------------------------------------------------------------------- #
# Missing-endpoint heuristics (migrated from old datelib.py)
# ------------------------------------------------------------------------- #


def _fill_missing(start_year: int | None, end_year: int | None) -> DateRange:
    """Apply the old +/-200-year fallback for open-ended intervals."""
    if end_year is None and isinstance(start_year, int):
        end_year = min(_LATEST_YEAR_FALLBACK, start_year + _DEFAULT_GAP)

    if start_year is None and isinstance(end_year, int):
        start_year = end_year - _DEFAULT_GAP

    return start_year, end_year


# ------------------------------------------------------------------------- #
# Public API (same signatures as the old datelib.py)
# ------------------------------------------------------------------------- #


def process_edtf_date(simplified_date_statement: str, date_statement: str) -> DateRange:
    """Parse an EDTF string and return the year range.

    Tries strict parsing first, then falls back to natlang coercion.
    """
    result = antequem.parse(simplified_date_statement)

    if result.is_err:
        coerced = natlang_coerce(date_statement)
        if coerced is None:
            log.debug(
                "Parsing failed for %s, simplified to %s",
                date_statement,
                simplified_date_statement,
            )
            return None, None

        log.debug("Coerced %s -> %s", date_statement, coerced)
        result = antequem.parse(coerced)
        if result.is_err:
            log.debug(
                "Parsing failed after coercion for %s (%s)",
                date_statement,
                simplified_date_statement,
            )
            return None, None

    parsed = result.unwrap()
    start_year = antequem.lower_year(parsed)
    end_year = antequem.upper_year(parsed)

    if start_year is not None and end_year is not None and start_year > end_year:
        log.warning(
            "Error parsing date: start %s > end %s from %s",
            start_year,
            end_year,
            date_statement,
        )
        return None, None

    return _fill_missing(start_year, end_year)


def parse_date_statement(date_statement: str) -> DateRange:
    """Parse a raw date statement into a year range.

    Fast-paths single years and simple ranges; falls back to
    ``process_edtf_date`` for complex expressions.
    """
    # Fast path: single four-digit year
    if len(date_statement) == 4 and date_statement.isdigit():
        year = int(date_statement)
        return year, year

    # Fast path: simple range "1234-5678"
    if "-" in date_statement:
        parts = date_statement.split("-")
        if len(parts) == 2 and all(p.isdigit() and len(p) == 4 for p in parts):
            return int(parts[0]), int(parts[1])

    # Fast path: single year after stripping leading hyphen
    if date_statement.startswith("-"):
        candidate = date_statement[1:]
        if candidate.isdigit() and len(candidate) == 4:
            return process_edtf_date(f"/{candidate}", date_statement)

    if match := _OPEN_RIGHT_SUFFIX_RANGE_RE.fullmatch(date_statement):
        start_year = int(match.group("first"))
        lower_bound = int(match.group("second"))
        upper_bound = min(_LATEST_YEAR_FALLBACK, lower_bound + _DEFAULT_GAP)
        return start_year, upper_bound

    if match := _OPEN_LEFT_SUFFIX_RANGE_RE.fullmatch(date_statement):
        lower_bound = int(match.group("first"))
        end_year = int(match.group("second"))
        if match.group("second_mark").lower() == "p":
            end_year = min(_LATEST_YEAR_FALLBACK, end_year + _DEFAULT_GAP)
        return lower_bound, end_year

    if match := _OPEN_RIGHT_LIFE_MARKER_RE.fullmatch(date_statement):
        lower_bound = int(match.group("year"))
        upper_bound = min(_LATEST_YEAR_FALLBACK, lower_bound + _DEFAULT_GAP)
        return lower_bound, upper_bound

    # Skip known "no date" markers (handled by coerce returning None)
    if antequem.natlang.is_no_date(date_statement):
        return None, None

    return process_edtf_date(date_statement, date_statement)


def process_date_statements(
    date_statements: list[str], record_id: str
) -> list[int] | None:
    """Process multiple date statements and return [earliest, latest]."""
    earliest_dates: list[int] = []
    latest_dates: list[int] = []

    for statement in date_statements:
        if antequem.natlang.is_no_date(statement):
            continue

        if statement.startswith("-"):
            log.warning(
                "Leading hyphen in date %s for record %s",
                statement,
                record_id,
            )

        if "\u200f" in statement:
            log.warning(
                "RTL character in date %s for record %s",
                statement,
                record_id,
            )

        try:
            earliest, latest = parse_date_statement(statement)
        except Exception as e:
            log.warning("Error parsing date '%s' for %s: %s", statement, record_id, e)
            return None

        if earliest is None and latest is None:
            log.warning("Problem with date '%s' for record %s", statement, record_id)
            return None

        if earliest is not None:
            earliest_dates.append(earliest)
        if latest is not None:
            latest_dates.append(latest)

    if not earliest_dates or not latest_dates:
        return None

    earliest_date = max(min(earliest_dates), _EARLIEST_YEAR_FALLBACK)
    latest_date = min(max(latest_dates), _LATEST_YEAR_FALLBACK)

    if (
        earliest_date <= _EARLIEST_YEAR_FALLBACK
        and latest_date >= _LATEST_YEAR_FALLBACK
    ):
        return None

    if earliest_date > latest_date:
        log.warning(
            "Earliest %s > latest %s for record %s",
            earliest_date,
            latest_date,
            record_id,
        )
        return None

    if earliest_date < 0 and latest_date > 300:
        log.warning(
            "Unlikely earliest date %s; setting to [%s, %s] for record %s",
            earliest_date,
            latest_date - _DECADE_GAP,
            latest_date,
            record_id,
        )
        # Set the earliest date a decade earlier than the latest
        # The most likely cause of this is a date like "before 1810"
        # where the start date is open-ended.
        earliest_date = latest_date - _DECADE_GAP

    return [earliest_date, latest_date]
