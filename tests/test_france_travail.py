import json
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

import ingestion.fetch_france_travail as france_travail_module
from ingestion.fetch_france_travail import fetch_france_travail_offers


def _reset_token_cache() -> None:
    france_travail_module._TOKEN_CACHE["access_token"] = None
    france_travail_module._TOKEN_CACHE["expires_at"] = None


class FakeResponse:
    def __init__(self, url: str, payload: dict, status_code: int = 200):
        self.status_code = status_code
        self.url = url
        self._payload = payload

    def raise_for_status(self):
        if self.status_code >= 400:
            raise RuntimeError(f"HTTP {self.status_code}")
        return None

    def json(self):
        return self._payload


def test_fetch_france_travail_offers_saves_raw_payload(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("FRANCE_TRAVAIL_CLIENT_ID", "test-client")
    monkeypatch.setenv("FRANCE_TRAVAIL_CLIENT_SECRET", "test-secret")
    monkeypatch.setenv("FRANCE_TRAVAIL_OAUTH_TOKEN_URL", "https://auth.francetravail.io/oauth2/token")
    _reset_token_cache()

    def fake_post(url, data=None, headers=None, timeout=None, **kwargs):
        assert url == "https://auth.francetravail.io/oauth2/token"
        assert data["grant_type"] == "client_credentials"
        assert data["client_id"] == "test-client"
        assert data["client_secret"] == "test-secret"
        return FakeResponse(url=url, payload={"access_token": "token-123", "expires_in": 3600})

    def fake_get(endpoint, params=None, headers=None, timeout=None, **kwargs):
        assert params["range"] == "0-49"
        assert params["departement"] == 44
        assert params["motsCles"] == "Data Engineer DevOps Cloud"
        assert headers["Authorization"] == "Bearer token-123"
        return FakeResponse(
            url=endpoint,
            payload={"nbResultats": 1, "resultats": [{"title": "Data Engineer Nantes"}]},
        )

    monkeypatch.setattr("ingestion.fetch_france_travail.requests.post", fake_post)
    monkeypatch.setattr("ingestion.fetch_france_travail.requests.get", fake_get)

    payload = fetch_france_travail_offers()

    assert payload["count"] == 1
    saved_files = list((tmp_path / "data" / "raw").glob("france_travail_*.json"))
    assert len(saved_files) == 1
    loaded = json.loads(saved_files[0].read_text(encoding="utf-8"))
    assert loaded == {"nbResultats": 1, "resultats": [{"title": "Data Engineer Nantes"}]}


def test_fetch_france_travail_offers_refreshes_token_on_401(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("FRANCE_TRAVAIL_CLIENT_ID", "test-client")
    monkeypatch.setenv("FRANCE_TRAVAIL_CLIENT_SECRET", "test-secret")
    monkeypatch.setenv("FRANCE_TRAVAIL_OAUTH_TOKEN_URL", "https://auth.francetravail.io/oauth2/token")
    _reset_token_cache()

    token_calls = []
    request_calls = []

    def fake_post(url, data=None, headers=None, timeout=None, **kwargs):
        token_calls.append((data or {}).get("client_id"))
        return FakeResponse(url=url, payload={"access_token": f"token-{len(token_calls)}", "expires_in": 3600})

    def fake_get(endpoint, params=None, headers=None, timeout=None, **kwargs):
        assert params["range"] == "0-49"
        assert params["departement"] == 44
        assert params["motsCles"] == "Data Engineer DevOps Cloud"
        request_calls.append(headers["Authorization"])
        if len(request_calls) == 1:
            return FakeResponse(url=endpoint, payload={"error": "Unauthorized"}, status_code=401)
        return FakeResponse(
            url=endpoint,
            payload={"nbResultats": 1, "resultats": [{"title": "Data Engineer Nantes"}]},
        )

    monkeypatch.setattr("ingestion.fetch_france_travail.requests.post", fake_post)
    monkeypatch.setattr("ingestion.fetch_france_travail.requests.get", fake_get)

    payload = fetch_france_travail_offers()

    assert payload["count"] == 1
    assert request_calls == ["Bearer token-1", "Bearer token-2"]
    assert len(token_calls) == 2


def test_fetch_france_travail_offers_rate_limit_raises(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("FRANCE_TRAVAIL_CLIENT_ID", "test-client")
    monkeypatch.setenv("FRANCE_TRAVAIL_CLIENT_SECRET", "test-secret")
    _reset_token_cache()

    def fake_post(url, data=None, headers=None, timeout=None, **kwargs):
        return FakeResponse(url=url, payload={"access_token": "token-123", "expires_in": 3600})

    def fake_get(endpoint, params=None, headers=None, timeout=None, **kwargs):
        assert params["range"] == "0-49"
        assert params["departement"] == 44
        assert params["motsCles"] == "Data Engineer DevOps Cloud"
        return FakeResponse(url=endpoint, payload={"error": "Too Many Requests"}, status_code=429)

    monkeypatch.setattr("ingestion.fetch_france_travail.requests.post", fake_post)
    monkeypatch.setattr("ingestion.fetch_france_travail.requests.get", fake_get)

    with pytest.raises(RuntimeError, match="rate limited"):
        fetch_france_travail_offers()
