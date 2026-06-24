from scripts.check_authority_links import (
    REQUEST_TIMESTAMPS,
    RecordReference,
    build_authority_url,
    build_grouped_ids_from_failures,
    build_grouped_ids_from_refreshed_failures,
    build_request_url,
    build_report,
    build_updated_report,
    default_validator,
    fetch_solr_records_by_full_rism_id,
    get_exception_retry_delay,
    get_retry_delay,
    isni_validator,
    is_update_candidate,
    merge_refreshed_failures,
    merge_updated_failures,
    orcid_validator,
    parse_external_id,
    summarize_failures,
    service_matches_filters,
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


def test_build_request_url_uses_isni_sru_endpoint():
    assert build_request_url("isni", "0000000116557629") == (
        "http://isni.oclc.org/sru/DB=1.2/?"
        "query=pica.isn+%3D+%220000000116557629%22&"
        "operation=searchRetrieve&recordSchema=isni-b"
    )


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


def test_isni_validator_accepts_matching_sru_xml():
    response = FakeResponse(
        200,
        """<?xml version="1.0" encoding="UTF-8" ?>
<srw:searchRetrieveResponse xmlns:srw="http://www.loc.gov/zing/srw/">
  <srw:numberOfRecords>1</srw:numberOfRecords>
  <srw:records>
    <srw:record>
      <srw:recordData>
        <responseRecord>
          <ISNIAssigned>
            <isniUnformatted>0000000116557629</isniUnformatted>
          </ISNIAssigned>
        </responseRecord>
      </srw:recordData>
    </srw:record>
  </srw:records>
</srw:searchRetrieveResponse>""",
    )
    result = isni_validator(
        response,
        "0000000116557629",
        "https://isni.org/isni/0000000116557629",
        "isni:0000000116557629",
    )
    assert result.ok is True


def test_isni_validator_accepts_merged_isni():
    response = FakeResponse(
        200,
        """<?xml version="1.0" encoding="UTF-8" ?>
<srw:searchRetrieveResponse xmlns:srw="http://www.loc.gov/zing/srw/">
  <srw:numberOfRecords>1</srw:numberOfRecords>
  <srw:records>
    <srw:record>
      <srw:recordData>
        <responseRecord>
          <ISNIAssigned>
            <isniUnformatted>0000000439370260</isniUnformatted>
            <mergedISNI>0000000116557629</mergedISNI>
          </ISNIAssigned>
        </responseRecord>
      </srw:recordData>
    </srw:record>
  </srw:records>
</srw:searchRetrieveResponse>""",
    )
    result = isni_validator(
        response,
        "0000000116557629",
        "https://isni.org/isni/0000000116557629",
        "isni:0000000116557629",
    )
    assert result.ok is True


def test_isni_validator_reports_missing_record():
    response = FakeResponse(
        200,
        """<?xml version="1.0" encoding="UTF-8" ?>
<srw:searchRetrieveResponse xmlns:srw="http://www.loc.gov/zing/srw/">
  <srw:numberOfRecords>0</srw:numberOfRecords>
</srw:searchRetrieveResponse>""",
    )
    result = isni_validator(
        response,
        "0000000116557629",
        "https://isni.org/isni/0000000116557629",
        "isni:0000000116557629",
    )
    assert result.ok is False
    assert result.failure_type == "soft_404"


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


def test_wait_for_service_rate_limit_records_timestamp_for_iccu():
    REQUEST_TIMESTAMPS.clear()
    wait_for_service_rate_limit("iccu")
    assert "iccu" in REQUEST_TIMESTAMPS


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


def test_service_matches_filters_only_checks_service_selection():
    failure = {"service": "orcid", "http_status": 404}
    assert service_matches_filters(failure, None, None) is True
    assert service_matches_filters(failure, {"orcid"}, None) is True
    assert service_matches_filters(failure, {"dnb"}, None) is False
    assert service_matches_filters(failure, None, {"orcid"}) is False


def test_fetch_solr_records_by_full_rism_id_uses_terms_filter_query(monkeypatch):
    captured = {}

    class FakeRequestBuilder:
        def __init__(self):
            self._headers = {}
            self._body = ""

        def headers(self, headers):
            self._headers = headers
            return self

        def body_text(self, body):
            self._body = body
            return self

        def build(self):
            return self

        def send(self):
            captured["headers"] = self._headers
            captured["body"] = self._body
            return FakeResponse(
                200,
                json_body={
                    "response": {
                        "docs": [
                            {
                                "full_rism_id": "people/100",
                                "type": "person",
                                "external_ids": ["lc:n1"],
                            }
                        ]
                    }
                },
            )

    class FakeClient:
        def post(self, url):
            captured["url"] = url
            return FakeRequestBuilder()

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

    class FakeBuilder:
        def gzip(self, enabled):
            return self

        def deflate(self, enabled):
            return self

        def follow_redirects(self, enabled):
            return self

        def build(self):
            return FakeClient()

    monkeypatch.setattr("scripts.check_authority_links.SyncClientBuilder", FakeBuilder)
    records = fetch_solr_records_by_full_rism_id(
        "http://localhost:8983/solr/muscatplus_live",
        {"people/100", "works/200"},
    )
    assert records["people/100"]["external_ids"] == ["lc:n1"]
    assert captured["url"].endswith("/select")
    assert captured["headers"] == {
        "Content-Type": "application/x-www-form-urlencoded"
    }
    assert "q=%2A%3A%2A" in captured["body"]
    assert "fq=%7B%21terms+f%3Dfull_rism_id%7D" in captured["body"]
    assert "people%2F100%2Cworks%2F200" in captured["body"]


def test_build_grouped_ids_from_failures_groups_all_matching_services():
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


def test_build_grouped_ids_from_refreshed_failures_uses_current_solr_ids():
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
            "rism_id": "200",
            "full_rism_id": "people/200",
            "record_type": "person",
            "external_id": "dnb:999",
            "service": "dnb",
            "http_status": 429,
        },
        {
            "rism_id": "300",
            "full_rism_id": "people/300",
            "record_type": "person",
            "external_id": "dnb:555",
            "service": "dnb",
            "http_status": 404,
        },
    ]
    records_by_full_rism_id = {
        "people/100": {
            "id": "100",
            "full_rism_id": "people/100",
            "type": "person",
            "external_ids": ["dnb:123", "dnb:456", "wkp:Q1"],
        },
        "people/200": {
            "id": "200",
            "full_rism_id": "people/200",
            "type": "person",
            "external_ids": ["wkp:Q2"],
        },
        "people/300": {
            "id": "300",
            "full_rism_id": "people/300",
            "type": "person",
            "external_ids": ["dnb:777"],
        },
    }
    grouped_ids, rebuilt_scopes, discovered_current_ids = (
        build_grouped_ids_from_refreshed_failures(
            failures,
            records_by_full_rism_id,
            None,
            None,
        )
    )
    assert sorted(grouped_ids) == ["dnb:123", "dnb:456", "dnb:777"]
    assert rebuilt_scopes == {
        ("people/100", "dnb"),
        ("people/200", "dnb"),
        ("people/300", "dnb"),
    }
    assert discovered_current_ids == 3
    assert [reference.full_rism_id for reference in grouped_ids["dnb:456"]] == [
        "people/100"
    ]


def test_build_grouped_ids_from_refreshed_failures_marks_missing_solr_records_for_removal():
    failures = [
        {
            "rism_id": "100",
            "full_rism_id": "people/100",
            "record_type": "person",
            "external_id": "lc:n1",
            "service": "lc",
            "http_status": 404,
        }
    ]
    grouped_ids, rebuilt_scopes, discovered_current_ids = (
        build_grouped_ids_from_refreshed_failures(
            failures,
            records_by_full_rism_id={},
            selected_services={"lc"},
            skipped_services=None,
        )
    )
    assert grouped_ids == {}
    assert rebuilt_scopes == {("people/100", "lc")}
    assert discovered_current_ids == 0


def test_merge_refreshed_failures_removes_stale_and_replaces_refreshed_rows():
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
            "external_id": "dnb:999",
            "service": "dnb",
            "failure_type": "http_error",
            "http_status": 429,
            "reason": "Will disappear",
            "url": "https://d-nb.info/gnd/999",
        },
        {
            "rism_id": "300",
            "full_rism_id": "people/300",
            "record_type": "person",
            "external_id": "wkp:Q1",
            "service": "wkp",
            "failure_type": "http_error",
            "http_status": 404,
            "reason": "Keep me",
            "url": "https://www.wikidata.org/wiki/Q1",
        },
        {
            "rism_id": "400",
            "full_rism_id": "people/400",
            "record_type": "person",
            "external_id": "orcid:0000-0002-1825-0097",
            "service": "orcid",
            "failure_type": "soft_404",
            "http_status": 404,
            "reason": "Old ORCID failure",
            "url": "https://orcid.org/0000-0002-1825-0097",
        },
    ]
    refreshed_failures = [
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
    merged, removed_count = merge_refreshed_failures(
        existing_failures,
        refreshed_failures,
        {
            ("people/100", "dnb"),
            ("people/200", "dnb"),
            ("people/400", "orcid"),
        },
        None,
        None,
    )
    assert removed_count == 3
    assert len(merged) == 2
    assert merged[0]["external_id"] == "dnb:123"
    assert merged[0]["http_status"] == 404
    assert merged[1]["external_id"] == "wkp:Q1"


def test_merge_refreshed_failures_drops_rows_that_now_validate():
    existing_failures = [
        {
            "rism_id": "100",
            "full_rism_id": "people/100",
            "record_type": "person",
            "external_id": "iccu:MUSV051790",
            "service": "iccu",
            "failure_type": "request_error",
            "http_status": None,
            "reason": "Old transport failure",
            "url": "http://id.sbn.it/bid/MUSV051790",
        }
    ]
    merged, removed_count = merge_refreshed_failures(
        existing_failures,
        refreshed_failures=[],
        rebuilt_scopes={("people/100", "iccu")},
        selected_services={"iccu"},
        skipped_services=None,
    )
    assert removed_count == 1
    assert merged == []


def test_merge_refreshed_failures_preserves_untargeted_service_on_same_record():
    existing_failures = [
        {
            "rism_id": "100",
            "full_rism_id": "people/100",
            "record_type": "person",
            "external_id": "dnb:123",
            "service": "dnb",
            "failure_type": "http_error",
            "http_status": 429,
            "reason": "Old DNB failure",
            "url": "https://d-nb.info/gnd/123",
        },
        {
            "rism_id": "100",
            "full_rism_id": "people/100",
            "record_type": "person",
            "external_id": "wkp:Q1",
            "service": "wkp",
            "failure_type": "http_error",
            "http_status": 404,
            "reason": "Keep me",
            "url": "https://www.wikidata.org/wiki/Q1",
        },
    ]
    merged, removed_count = merge_refreshed_failures(
        existing_failures,
        refreshed_failures=[],
        rebuilt_scopes={("people/100", "dnb")},
        selected_services={"dnb"},
        skipped_services=None,
    )
    assert removed_count == 1
    assert merged == [existing_failures[1]]


def test_merge_updated_failures_replaces_and_removes_targeted_failures():
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
    assert len(merged) == 1
    assert merged[0]["http_status"] == 404
    assert merged[0]["external_id"] == "dnb:123"


def test_merge_updated_failures_drops_rows_that_now_validate():
    existing_failures = [
        {
            "rism_id": "100",
            "full_rism_id": "people/100",
            "record_type": "person",
            "external_id": "orcid:0000-0002-1825-0097",
            "service": "orcid",
            "failure_type": "soft_404",
            "http_status": 404,
            "reason": "Old ORCID failure",
            "url": "https://orcid.org/0000-0002-1825-0097",
        }
    ]
    merged = merge_updated_failures(
        existing_failures,
        updated_failures=[],
        selected_services={"orcid"},
        skipped_services=None,
    )
    assert merged == []


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
