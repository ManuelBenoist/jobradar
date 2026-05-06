import importlib
import json
import os
from unittest.mock import MagicMock, patch

import pytest
import requests

mod = importlib.import_module("src.lambda.jooble.ingest_jooble")
fetch_jooble_jobs = mod.fetch_jooble_jobs
lambda_handler = mod.lambda_handler


class TestFetchJoobleJobs:
    def test_successful_fetch(self):
        mock_response = MagicMock()
        mock_response.json.return_value = {
            "totalCount": 3,
            "jobs": [
                {"id": "j1", "title": "Data Engineer", "company": "Corp", "location": "Nantes"},
                {"id": "j2", "title": "Data Scientist", "company": "Inc", "location": "Paris"},
                {"id": "j3", "title": "ML Engineer", "company": "Ltd", "location": "Remote"},
            ],
        }
        mock_response.raise_for_status = MagicMock()

        with patch("requests.post", return_value=mock_response) as mock_post:
            result = fetch_jooble_jobs("Data Engineer", "Nantes", "test_key")

        assert result["count"] == 3
        assert len(result["results"]) == 3
        assert result["keyword"] == "Data Engineer"
        assert result["source"] == "jooble"
        assert "ingested_at" in result

        args, kwargs = mock_post.call_args
        assert "jooble.org" in args[0]
        assert kwargs["json"]["keywords"] == "Data Engineer"
        assert kwargs["json"]["location"] == "Nantes"

    def test_request_exception_raises(self):
        with patch("requests.post", side_effect=requests.exceptions.RequestException("Jooble down")):
            with pytest.raises(requests.exceptions.RequestException, match="Jooble down"):
                fetch_jooble_jobs("Data", "Nantes", "key")

    def test_empty_jobs_list(self):
        mock_response = MagicMock()
        mock_response.json.return_value = {"totalCount": 0, "jobs": []}
        mock_response.raise_for_status = MagicMock()

        with patch("requests.post", return_value=mock_response):
            result = fetch_jooble_jobs("Data", "Nantes", "key")

        assert result["count"] == 0
        assert result["results"] == []


class TestLambdaHandler:
    def test_successful_invocation(self, mock_s3_client, sample_event):
        os.environ["JOOBLE_API_KEY"] = "test_key"
        mod.s3_client = mock_s3_client

        with patch.object(mod, "fetch_jooble_jobs") as mock_fetch:
            mock_fetch.return_value = {
                "count": 2,
                "results": [{"id": "j1"}, {"id": "j2"}],
                "keyword": "Data Engineer",
                "source": "jooble",
                "ingested_at": "2025-01-01T00:00:00",
            }
            result = lambda_handler(sample_event, None)

        assert result["statusCode"] == 200
        body = json.loads(result["body"])
        assert body["count"] == 2
        assert "jooble" in body["file"]
        assert mock_s3_client.put_object.called

    def test_exception_propagates(self, mock_s3_client, sample_event):
        os.environ["JOOBLE_API_KEY"] = "key"
        mod.s3_client = mock_s3_client

        with patch.object(
            mod, "fetch_jooble_jobs", side_effect=Exception("Jooble error")
        ), pytest.raises(Exception, match="Jooble error"):
            lambda_handler(sample_event, None)
