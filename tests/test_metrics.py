import pytest
from urllib.parse import parse_qs, urlparse

from indexer.helpers import solr
from indexer.helpers.metrics import (
    calculate_metric_outcome,
    render_metrics,
    write_metrics_atomically,
)


class FakeResponse:
    def __init__(self, status: int, body: dict) -> None:
        self.status = status
        self.body = body

    def json(self) -> dict:
        return self.body


class FakeRequest:
    def __init__(self, response: FakeResponse) -> None:
        self.response = response

    def build(self) -> "FakeRequest":
        return self

    def send(self) -> FakeResponse:
        return self.response


class FakeClient:
    def __init__(self, responses: list[FakeResponse]) -> None:
        self.responses = responses
        self.urls: list[str] = []

    def get(self, url: str) -> FakeRequest:
        self.urls.append(url)
        return FakeRequest(self.responses.pop(0))

    def __enter__(self) -> "FakeClient":
        return self

    def __exit__(self, *_: object) -> None:
        return None


class FakeSyncClientBuilder:
    client: FakeClient

    def build(self) -> FakeClient:
        return self.client


def test_render_metrics_includes_run_and_record_type_gauges() -> None:
    rendered = render_metrics(
        "muscatplus_indexer",
        success=True,
        finished_unixtime=1730000000,
        duration_seconds=12.5,
        errors=2,
        indexed_counts={("muscat", "sources"): 2, ("diamm", "people"): 1},
    )

    assert "# TYPE muscatplus_indexer_last_run_success gauge" in rendered
    assert "muscatplus_indexer_last_run_success 1" in rendered
    assert "muscatplus_indexer_last_finished_unixtime 1730000000" in rendered
    assert "muscatplus_indexer_last_run_duration_seconds 12.500000" in rendered
    assert "muscatplus_indexer_last_run_errors 2" in rendered
    assert "muscatplus_indexer_last_run_documents_submitted" not in rendered
    assert (
        'muscatplus_indexer_last_run_records_indexed{project="muscat",record_type="sources"} 2'
        in rendered
    )


def test_write_metrics_atomically_replaces_existing_file(tmp_path) -> None:
    final_path = tmp_path / "muscatplus_indexer.prom"
    final_path.write_text("old\n")

    write_metrics_atomically(str(tmp_path), "muscatplus_indexer", "new\n")

    assert final_path.read_text() == "new\n"
    assert list(tmp_path.glob(".*.tmp")) == []


def test_invalid_job_name_is_rejected(tmp_path) -> None:
    with pytest.raises(ValueError, match="valid Prometheus"):
        write_metrics_atomically(str(tmp_path), "invalid-name", "content\n")


def test_final_record_counts_query_the_final_source_documents(monkeypatch) -> None:
    response_count = len(solr.FINAL_RECORD_COUNT_QUERIES)
    FakeSyncClientBuilder.client = FakeClient(
        [FakeResponse(200, {"response": {"numFound": 7}}) for _ in range(response_count)]
    )
    monkeypatch.setattr(solr, "SyncClientBuilder", FakeSyncClientBuilder)

    counts, errors = solr.get_final_record_counts(
        {"solr": {"server": "http://solr"}, "indexing_core": "indexing"}
    )

    source_query = parse_qs(urlparse(FakeSyncClientBuilder.client.urls[0]).query)["q"]
    assert source_query == ["type:source AND -project_s:[* TO *]"]
    assert counts[("muscat", "sources")] == 7
    assert errors == 0


def test_final_record_count_failure_is_reported_without_count(monkeypatch) -> None:
    successful_responses = [
        FakeResponse(200, {"response": {"numFound": 7}})
        for _ in range(len(solr.FINAL_RECORD_COUNT_QUERIES) - 1)
    ]
    FakeSyncClientBuilder.client = FakeClient([FakeResponse(500, {})] + successful_responses)
    monkeypatch.setattr(solr, "SyncClientBuilder", FakeSyncClientBuilder)

    counts, errors = solr.get_final_record_counts(
        {"solr": {"server": "http://solr"}, "indexing_core": "indexing"}
    )

    assert ("muscat", "sources") not in counts
    assert errors == 1


def test_count_query_errors_do_not_change_index_success() -> None:
    success, errors = calculate_metric_outcome(True, indexing_errors=0, count_errors=1)

    assert success is True
    assert errors == 1
