from datetime import UTC, datetime

from cantus_indexer.records.source import create_source_index_documents


def test_cantus_manuscript_holding_has_a_stable_identifier() -> None:
    record = {
        "id": 1000476,
        "shelfmark": "MS 1",
        "institution_siglum": "CH-Zz",
        "institution_rism_ids": [],
        "source_century": [],
        "source_summary": None,
        "html_source_description": None,
        "source_name": "Test source",
        "institution_name": "Test institution",
        "institution_city": "Zurich",
        "institution_id": 42,
        "created": datetime(2026, 1, 1, tzinfo=UTC),
        "updated": datetime(2026, 1, 1, tzinfo=UTC),
    }

    _, holding = create_source_index_documents(record, {})

    assert holding["id"] == "cantus_holding_1000476"  # noqa: S101
