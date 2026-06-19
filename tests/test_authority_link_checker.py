from scripts.check_authority_links import (
    REQUEST_TIMESTAMPS,
    RecordReference,
    build_authority_url,
    build_grouped_ids_from_failures,
    build_request_url,
    build_report,
    build_updated_report,
    default_validator,
    get_exception_retry_delay,
    get_retry_delay,
    is_update_candidate,
    merge_updated_failures,
    orcid_validator,
    parse_external_id,
    summarize_failures,
    validate_external_id,
    wait_for_service_rate_limit,
    wikidata_validator,
)


class FakeResponse:
    def __init__(self, status: int, text: str = "", json_body=None, headers=None):
        self.status = status
        self._text = text
        self._json_body = json_body
        self._headers = headers or {}

    def text(self):
        return self._text

    def json(self):
        if isinstance(self._json_body, Exception):
            raise self._json_body
        return self._json_body

    def get_header(self, name: str):
        return self._headers.get(name.lower())


def test_parse_external_id_splits_on_first_colon():
    assert parse_external_id("foo:bar:baz") == ("foo", "bar:baz")


def test_parse_external_id_rejects_invalid_values():
    try:
        parse_external_id("viaf")
    except ValueError as exc:
        assert "Malformed external id" in str(exc)
    else:
        raise AssertionError("Expected ValueError")


def test_build_authority_url_uses_template():
    assert build_authority_url("dnb", "118584596") == "https://d-nb.info/gnd/118584596"


def test_build_request_url_uses_lc_json_endpoint():
    assert build_request_url("lc", "n86857160") == "http://id.loc.gov/authorities/names/n86857160.skos.json"


def test_build_request_url_uses_bne_jsonld_endpoint():
    assert build_request_url("bne", "XX4676341") == "https://datos.bne.es/resource/XX4676341.jsonld"


def test_default_validator_detects_cloudflare_block():
    response = FakeResponse(
        403,
        "<html><title>Cloudflare</title><h1>You have been blocked</h1></html>",
    )
    result = default_validator(response, "123", "https://viaf.org/viaf/123", "viaf:123")
    assert result.ok is False
    assert result.failure_type == "service_blocked"


def test_wikidata_validator_detects_soft_404():
    response = FakeResponse(200, "<p>This entity does not exist. You can:</p>")
    result = wikidata_validator(
        response,
        "Q999999",
        "https://www.wikidata.org/wiki/Q999999",
        "wkp:Q999999",
    )
    assert result.ok is False
    assert result.failure_type == "soft_404"


def test_orcid_validator_accepts_matching_json():
    response = FakeResponse(
        200,
        "",
        {"orcid-identifier": {"path": "0000-0002-1825-0097"}},
    )
    result = orcid_validator(
        response,
        "0000-0002-1825-0097",
        "https://orcid.org/0000-0002-1825-0097",
        "orcid:0000-0002-1825-0097",
    )
    assert result.ok is True


def test_validate_external_id_marks_missing_template_as_skipped():
    result = validate_external_id("corago:0000123")
    assert result.skipped is True
    assert result.failure_type == "missing_url_template"


def test_get_retry_delay_uses_numeric_retry_after():
    response = FakeResponse(429, headers={"retry-after": "7"})
    assert get_retry_delay(response, 1, "dnb") == 7.0


def test_get_retry_delay_falls_back_when_retry_after_missing():
    response = FakeResponse(429)
    assert get_retry_delay(response, 2, "dnb") == 1.0


def test_get_retry_delay_ignores_retry_after_for_viaf():
    response = FakeResponse(429, headers={"retry-after": "53902"})
    assert get_retry_delay(response, 1, "viaf") == 0.5


def test_get_retry_delay_caps_long_retry_after():
    response = FakeResponse(429, headers={"retry-after": "53902"})
    assert get_retry_delay(response, 1, "dnb") == 30.0


def test_get_exception_retry_delay_uses_incremental_backoff():
    assert get_exception_retry_delay(2) == 1.0


def test_wait_for_service_rate_limit_skips_unconfigured_service():
    wait_for_service_rate_limit("dnb")


def test_wait_for_service_rate_limit_records_timestamp_for_lc():
    REQUEST_TIMESTAMPS.clear()
    wait_for_service_rate_limit("lc")
    assert "lc" in REQUEST_TIMESTAMPS


def test_is_update_candidate_matches_retryable_update_statuses_and_filters():
    failure = {"service": "dnb", "http_status": 429}
    assert is_update_candidate(failure, None, None) is True
    assert is_update_candidate(failure, {"dnb"}, None) is True
    assert is_update_candidate(failure, {"wkp"}, None) is False
    assert is_update_candidate(failure, None, {"dnb"}) is False
    assert is_update_candidate({"service": "dnb", "http_status": 404}, None, None) is False
    assert is_update_candidate({"service": "dnb", "http_status": 503}, None, None) is True
    assert is_update_candidate(
        {"service": "wkp", "failure_type": "request_error", "http_status": None},
        None,
        None,
    ) is True


def test_build_grouped_ids_from_failures_groups_only_matching_update_statuses():
    failures = [
        {
            "rism_id": "100",
            "full_rism_id": "people/100",
            "record_type": "person",
            "external_id": "dnb:123",
            "service": "dnb",
            "http_status": 429,
        },
        {
            "rism_id": "101",
            "full_rism_id": "people/101",
            "record_type": "person",
            "external_id": "dnb:123",
            "service": "dnb",
            "http_status": 429,
        },
        {
            "rism_id": "102",
            "full_rism_id": "people/102",
            "record_type": "person",
            "external_id": "wkp:Q1",
            "service": "wkp",
            "http_status": 503,
        },
        {
            "rism_id": "103",
            "full_rism_id": "people/103",
            "record_type": "person",
            "external_id": "wkp:Q2",
            "service": "wkp",
            "failure_type": "request_error",
            "http_status": None,
        },
    ]
    grouped = build_grouped_ids_from_failures(failures, {"dnb"}, None)
    assert list(grouped) == ["dnb:123"]
    assert [reference.full_rism_id for reference in grouped["dnb:123"]] == [
        "people/100",
        "people/101",
    ]
    grouped_all = build_grouped_ids_from_failures(failures, None, None)
    assert sorted(grouped_all) == ["dnb:123", "wkp:Q1", "wkp:Q2"]


def test_merge_updated_failures_replaces_and_removes_targeted_429s():
    existing_failures = [
        {
            "rism_id": "100",
            "full_rism_id": "people/100",
            "record_type": "person",
            "external_id": "dnb:123",
            "service": "dnb",
            "failure_type": "http_error",
            "http_status": 429,
            "reason": "Old 429",
            "url": "https://d-nb.info/gnd/123",
        },
        {
            "rism_id": "200",
            "full_rism_id": "people/200",
            "record_type": "person",
            "external_id": "wkp:Q1",
            "service": "wkp",
            "failure_type": "http_error",
            "http_status": 404,
            "reason": "Keep me",
            "url": "https://www.wikidata.org/wiki/Q1",
        },
        {
            "rism_id": "101",
            "full_rism_id": "people/101",
            "record_type": "person",
            "external_id": "dnb:456",
            "service": "dnb",
            "failure_type": "http_error",
            "http_status": 429,
            "reason": "Resolved later",
            "url": "https://d-nb.info/gnd/456",
        },
    ]
    updated_failures = [
        {
            "rism_id": "100",
            "full_rism_id": "people/100",
            "record_type": "person",
            "external_id": "dnb:123",
            "service": "dnb",
            "identifier": "123",
            "failure_type": "http_error",
            "http_status": 404,
            "reason": "Unexpected HTTP status 404.",
            "url": "https://d-nb.info/gnd/123",
        }
    ]
    merged = merge_updated_failures(existing_failures, updated_failures, None, None)
    assert len(merged) == 2
    assert merged[0]["http_status"] == 404
    assert merged[0]["external_id"] == "dnb:123"
    assert merged[1]["external_id"] == "wkp:Q1"


def test_summarize_failures_counts_by_service():
    failures = [
        {"service": "dnb"},
        {"service": "dnb"},
        {"service": "wkp"},
    ]
    assert summarize_failures(failures) == {"dnb": 2, "wkp": 1}


def test_build_updated_report_refreshes_failure_summary_only():
    report = {
        "generated_at": "old",
        "duration_seconds": 10,
        "solr_core": "muscatplus_live",
        "documents_scanned": 5,
        "links_checked": 10,
        "links_failed": 2,
        "links_skipped": 1,
        "services_checked": {"dnb": 10},
        "service_failures": {"dnb": 2},
        "service_skipped": {"corago": 1},
        "failures": [],
    }
    updated_failures = [
        {
            "rism_id": "100",
            "full_rism_id": "people/100",
            "record_type": "person",
            "external_id": "dnb:123",
            "service": "dnb",
            "identifier": "123",
            "failure_type": "http_error",
            "http_status": 404,
            "reason": "Unexpected HTTP status 404.",
            "url": "https://d-nb.info/gnd/123",
        }
    ]
    updated_report = build_updated_report(report, updated_failures, 0.0)
    assert updated_report["links_failed"] == 1
    assert updated_report["service_failures"] == {"dnb": 1}
    assert updated_report["documents_scanned"] == 5
    assert updated_report["links_checked"] == 10
    assert updated_report["links_skipped"] == 1
    assert updated_report["services_checked"] == {"dnb": 10}
    assert updated_report["service_skipped"] == {"corago": 1}


def test_build_report_includes_summary_and_failures():
    summary = {
        "links_checked": 10,
        "links_failed": 1,
        "links_skipped": 2,
        "services_checked": {"dnb": 10},
        "service_failures": {"dnb": 1},
        "service_skipped": {"corago": 2},
    }
    failures = [
        {
            "rism_id": "100",
            "full_rism_id": "people/100",
            "record_type": "person",
            "external_id": "dnb:123",
            "service": "dnb",
            "identifier": "123",
            "url": "https://d-nb.info/gnd/123",
            "failure_type": "http_error",
            "http_status": 404,
            "reason": "Unexpected HTTP status 404.",
        }
    ]
    report = build_report("muscatplus_live", 5, failures, summary, 0.0)
    assert report["solr_core"] == "muscatplus_live"
    assert report["links_failed"] == 1
    assert report["failures"][0]["rism_id"] == "100"
