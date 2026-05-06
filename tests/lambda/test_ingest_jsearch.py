import importlib
import json
import os
from unittest.mock import MagicMock, patch

import pytest
import requests

mod = importlib.import_module("src.lambda.jsearch.ingest_jsearch")
fetch_jsearch_jobs = mod.fetch_jsearch_jobs
lambda_handler = mod.lambda_handler


class TestFetchJSearchJobs:
    def test_successful_fetch(self):
        mock_response = MagicMock()
        mock_response.json.return_value = {
            "data": [
                {
                    "job_id": "js1",
                    "job_title": "Data Engineer",
                    "employer_name": "Corp",
                    "job_city": "Nantes",
                },
                {
                    "job_id": "js2",
                    "job_title": "DevOps",
                    "employer_name": "Inc",
                    "job_city": "Paris",
                },
            ]
        }
        mock_response.raise_for_status = MagicMock()

        with patch("requests.get", return_value=mock_response) as mock_get:
            result = fetch_jsearch_jobs("Data Engineer", "Nantes", "test_key")

        assert result["count"] == 2
        assert len(result["results"]) == 2
        assert result["keyword"] == "Data Engineer"
        assert result["source"] == "jsearch"
        assert "ingested_at" in result

        args, kwargs = mock_get.call_args
        assert "jsearch.p.rapidapi.com" in args[0]
        assert kwargs["headers"]["X-RapidAPI-Key"] == "test_key"
        assert kwargs["params"]["query"] == "Data Engineer in Nantes"

    def test_request_exception_raises(self):
        with patch("requests.get", side_effect=requests.exceptions.RequestException("JSearch down")):
            with pytest.raises(requests.exceptions.RequestException, match="JSearch down"):
                fetch_jsearch_jobs("Data", "Nantes", "key")

    def test_empty_data_list(self):
        mock_response = MagicMock()
        mock_response.json.return_value = {"data": []}
        mock_response.raise_for_status = MagicMock()

        with patch("requests.get", return_value=mock_response):
            result = fetch_jsearch_jobs("Data", "Nantes", "key")

        assert result["count"] == 0
        assert result["results"] == []


class TestLambdaHandler:
    def test_successful_invocation(self, mock_s3_client, sample_event):
        os.environ["JSEARCH_API_KEY"] = "test_key"
        mod.s3_client = mock_s3_client

        with patch.object(mod, "fetch_jsearch_jobs") as mock_fetch:
            mock_fetch.return_value = {
                "count": 1,
                "results": [{"job_id": "js1"}],
                "keyword": "Data Engineer",
                "source": "jsearch",
                "ingested_at": "2025-01-01T00:00:00",
            }
            result = lambda_handler(sample_event, None)

        assert result["statusCode"] == 200
        body = json.loads(result["body"])
        assert body["count"] == 1
        assert "jsearch" in body["file"]
        assert mock_s3_client.put_object.called

    def test_exception_propagates(self, mock_s3_client, sample_event):
        os.environ["JSEARCH_API_KEY"] = "key"
        mod.s3_client = mock_s3_client

        with patch.object(
            mod, "fetch_jsearch_jobs", side_effect=Exception("JSearch error")
        ), pytest.raises(Exception, match="JSearch error"):
            lambda_handler(sample_event, None)
