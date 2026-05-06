import importlib
import json
import os
from unittest.mock import MagicMock, patch

import pytest
import requests

mod = importlib.import_module("src.lambda.adzuna.ingest_adzuna")
fetch_adzuna_jobs = mod.fetch_adzuna_jobs
lambda_handler = mod.lambda_handler


class TestFetchAdzunaJobs:
    def test_successful_fetch(self):
        page1 = MagicMock()
        page1.json.return_value = {
            "count": 2,
            "results": [
                {"id": "1", "title": "Data Engineer", "company": {"display_name": "Test Corp"}},
                {"id": "2", "title": "DevOps", "company": {"display_name": "Cloud Inc"}},
            ],
        }
        page1.raise_for_status = MagicMock()
        page2 = MagicMock()
        page2.json.return_value = {"count": 2, "results": []}
        page2.raise_for_status = MagicMock()

        with patch("requests.get", side_effect=[page1, page2]) as mock_get:
            result = fetch_adzuna_jobs("Data Engineer", "Nantes", "test_id", "test_key")

        assert result["count"] == 2
        assert len(result["results"]) == 2
        assert result["keyword"] == "Data Engineer"
        assert "ingested_at" in result
        assert mock_get.call_count == 2
        args, kwargs = mock_get.call_args
        assert "api.adzuna.com" in args[0]
        assert kwargs["params"]["app_id"] == "test_id"
        assert kwargs["params"]["app_key"] == "test_key"

    def test_pagination_stops_on_empty_page(self):
        page1 = MagicMock()
        page1.json.return_value = {"count": 1, "results": [{"id": "1"}]}
        page1.raise_for_status = MagicMock()

        page2 = MagicMock()
        page2.json.return_value = {"count": 1, "results": []}
        page2.raise_for_status = MagicMock()

        with patch("requests.get", side_effect=[page1, page2]) as mock_get:
            result = fetch_adzuna_jobs("Data Engineer", "Nantes", "id", "key")

        assert len(result["results"]) == 1
        assert mock_get.call_count == 2

    def test_pagination_respects_max_pages(self):
        responses = []
        for i in range(4):
            resp = MagicMock()
            resp.json.return_value = {"count": 10, "results": [{"id": str(i)}]}
            resp.raise_for_status = MagicMock()
            responses.append(resp)

        with patch("requests.get", side_effect=responses) as mock_get:
            result = fetch_adzuna_jobs("Data Engineer", "Nantes", "id", "key")

        assert len(result["results"]) == 3
        assert mock_get.call_count == 3

    def test_request_exception_returns_partial(self):
        responses = [
            MagicMock(**{"json.return_value": {"count": 0, "results": [{"id": "1"}]}, "raise_for_status.return_value": None}),
            requests.exceptions.RequestException("API error"),
        ]

        with patch("requests.get", side_effect=responses):
            result = fetch_adzuna_jobs("Data", "Paris", "id", "key")

        assert len(result["results"]) == 1


class TestLambdaHandler:
    def test_successful_invocation(self, mock_s3_client, sample_event):
        mod.s3_client = mock_s3_client

        with patch.object(mod, "fetch_adzuna_jobs") as mock_fetch:
            mock_fetch.return_value = {
                "count": 3,
                "results": [
                    {"id": "1", "title": "DE"},
                    {"id": "2", "title": "DS"},
                    {"id": "3", "title": "MLE"},
                ],
                "keyword": "Data Engineer",
                "ingested_at": "2025-01-01T00:00:00",
            }
            result = lambda_handler(sample_event, None)

        assert result["statusCode"] == 200
        body = json.loads(result["body"])
        assert body["count"] == 3
        assert "adzuna" in body["file"]
        assert mock_s3_client.put_object.called

    def test_missing_env_var_raises(self, mock_s3_client, sample_event):
        os.environ.pop("BUCKET_NAME", None)
        os.environ.pop("ADZUNA_APP_ID", None)

        with pytest.raises(KeyError):
            lambda_handler(sample_event, None)

    def test_fetch_exception_propagates(self, mock_s3_client, sample_event):
        mod.s3_client = mock_s3_client
        with patch.object(mod, "fetch_adzuna_jobs", side_effect=Exception("API down")):
            with pytest.raises(Exception, match="API down"):
                lambda_handler(sample_event, None)
