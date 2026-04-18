import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from ingestion.orchestrator import run_daily_ingestion


def test_run_daily_ingestion_calls_sources_with_query_label(monkeypatch):
    collected = []

    def fake_france_travail_offers(*args, **kwargs):
        collected.append(
            ("france_travail", kwargs.get("keywords"), kwargs.get("query_name"))
        )
        return {"count": 1, "source": "france_travail"}

    def fake_adzuna_jobs(*args, **kwargs):
        collected.append(("adzuna", kwargs.get("what"), kwargs.get("query_name")))
        return {"count": 2, "source": "adzuna"}

    monkeypatch.setattr(
        "ingestion.orchestrator.fetch_france_travail_offers", fake_france_travail_offers
    )
    monkeypatch.setattr("ingestion.orchestrator.fetch_adzuna_jobs", fake_adzuna_jobs)

    result = run_daily_ingestion(keywords=["Data Engineer"], throttle_seconds=0)

    assert result["france_travail/data_engineer"]["source"] == "france_travail"
    assert result["adzuna/data_engineer"]["source"] == "adzuna"
    assert collected[0] == ("france_travail", "Data Engineer", "data_engineer")
    assert collected[1] == ("adzuna", "Data Engineer", "data_engineer")
