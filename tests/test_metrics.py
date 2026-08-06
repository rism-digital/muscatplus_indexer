import pytest

from index import (
    format_duration,
    index_publications,
    index_works,
    metrics_config,
    selected_index_steps,
)
from indexer.helpers.metrics import render_metrics, write_metrics_atomically


def test_render_metrics_includes_run_and_record_type_gauges() -> None:
    rendered = render_metrics(
        "muscatplus_indexer",
        success=True,
        finished_unixtime=1730000000,
        duration_seconds=12.5,
        errors=2,
        counts={("muscat", "sources"): 3, ("diamm", "people"): 4},
    )

    assert "# TYPE muscatplus_indexer_last_run_success gauge" in rendered
    assert "muscatplus_indexer_last_run_success 1" in rendered
    assert "muscatplus_indexer_last_finished_unixtime 1730000000" in rendered
    assert "muscatplus_indexer_last_run_duration_seconds 12.500000" in rendered
    assert "muscatplus_indexer_last_run_errors 2" in rendered
    assert (
        'muscatplus_indexer_last_run_records_indexed{project="muscat",record_type="sources"} 3'
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


def test_index_group_selection_deduplicates_publication_dependency() -> None:
    groups = {
        "works": (("publications", index_publications), ("works", index_works)),
        "publications": (("publications", index_publications),),
    }

    assert selected_index_steps(None, [], groups) == [
        ("publications", index_publications),
        ("works", index_works),
    ]
    assert selected_index_steps(["works"], [], groups) == [
        ("publications", index_publications),
        ("works", index_works),
    ]
    assert selected_index_steps(["publications"], [], groups) == [
        ("publications", index_publications)
    ]


def test_format_duration_matches_indexer_log_format() -> None:
    assert format_duration(3661.25) == "01:01:01.25"


def test_metrics_config_returns_project_specific_copy() -> None:
    original = {"dry": False}
    metrics_queue = object()

    configured = metrics_config(original, metrics_queue, "diamm", "all")

    assert original == {"dry": False}
    assert configured["metrics_context"] == {
        "queue": metrics_queue,
        "project": "diamm",
        "record_type": "all",
    }
