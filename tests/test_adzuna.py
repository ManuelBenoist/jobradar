import json
import sys
from pathlib import Path


sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from ingestion.fetch_adzuna import fetch_adzuna_jobs


class FakeResponse:
    def __init__(self, url: str, payload: dict):
        self.status_code = 200
        self.url = url
        self._payload = payload

    def raise_for_status(self):
        return None

    def json(self):
        return self._payload


def test_fetch_adzuna_jobs_saves_raw_payload(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("ADZUNA_APP_ID", "test-id")
    monkeypatch.setenv("ADZUNA_APP_KEY", "test-key")

    def fake_get(endpoint, params, timeout):
        assert endpoint == "https://api.adzuna.com/v1/api/jobs/fr/search/1"
        assert params["what"] == "Data Engineer"
        assert params["where"] == "Nantes"
        assert params["distance"] == 20
        return FakeResponse(
            url=endpoint,
            payload={
                "count": 2,
                "results": [{"title": "Data Engineer"}, {"title": "DevOps"}],
            },
        )

    monkeypatch.setattr("ingestion.fetch_adzuna.requests.get", fake_get)

    payload = fetch_adzuna_jobs()

    assert payload["count"] == 2
    saved_files = list((tmp_path / "data" / "raw").glob("adzuna_*.json"))
    assert len(saved_files) == 1

    loaded = json.loads(saved_files[0].read_text(encoding="utf-8"))
    assert loaded == payload


def test_fetch_adzuna_jobs_fetch_all_pages(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("ADZUNA_APP_ID", "test-id")
    monkeypatch.setenv("ADZUNA_APP_KEY", "test-key")

    calls = []

    def fake_get(endpoint, params, timeout):
        calls.append((endpoint, params))
        if endpoint.endswith("/1"):
            return FakeResponse(
                url=endpoint,
                payload={
                    "count": 4,
                    "results": [{"title": "Data Engineer"}, {"title": "DevOps"}],
                },
            )
        return FakeResponse(
            url=endpoint,
            payload={
                "count": 4,
                "results": [{"title": "Data Engineer 2"}, {"title": "DevOps 2"}],
            },
        )

    monkeypatch.setattr("ingestion.fetch_adzuna.requests.get", fake_get)

    payload = fetch_adzuna_jobs(fetch_all=True, max_pages=2)

    assert payload["count"] == 4
    assert payload["pages"] == 2
    assert len(payload["results"]) == 4
    assert len(calls) == 2

    saved_files = list((tmp_path / "data" / "raw").glob("adzuna_*.json"))
    assert len(saved_files) == 2
