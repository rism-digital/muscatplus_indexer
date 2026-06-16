from indexer.helpers.parse_dates import parse_date_statement, process_date_statements


def test_single_year():
    assert parse_date_statement("1850") == (1850, 1850)


def test_unspecified_year():
    assert parse_date_statement("18XX") == (1800, 1899)


def test_before_suffix_uses_open_upper_bound_fallback():
    assert parse_date_statement("1811a") == (1761, 1811)


def test_after_suffix_uses_open_lower_bound_fallback():
    assert parse_date_statement("1811p") == (1811, 1861)


def test_leading_hyphen_means_before_year():
    assert parse_date_statement("-1910") == (1860, 1910)


def test_circa_year_stays_single_year():
    assert parse_date_statement("c1798") == (1798, 1798)


def test_century_expression():
    assert parse_date_statement("18th century") == (1701, 1800)


def test_century_turn_shorthand():
    assert parse_date_statement("18./19.") == (1790, 1810)


def test_bce_ce_range():
    assert parse_date_statement("0070 BCE-0019") == (-69, 19)


def test_open_right_suffix_range():
    assert parse_date_statement("1505c-1549p") == (1505, 1599)


def test_open_left_suffix_range():
    assert parse_date_statement("1770p-1815c") == (1770, 1815)


def test_open_left_open_right_suffix_range():
    assert parse_date_statement("1700p-1768p") == (1700, 1818)


def test_open_right_death_marker():
    assert parse_date_statement("1632p+") == (1632, 1682)


def test_copied_between_range():
    assert parse_date_statement("[copied between 1790 and 1810]") == (1790, 1810)


def test_parenthetical_century_appendage_on_range():
    assert parse_date_statement("1750-1799 (18.2d)") == (1750, 1799)


def test_multi_statement_aggregation():
    statements = ["1811a", "1850"]
    assert process_date_statements(statements, "person_1") == [1761, 1850]


def test_no_date_statement_list_returns_none():
    assert process_date_statements(["s.d.", "unknown"], "person_2") is None
