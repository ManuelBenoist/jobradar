import importlib
import json
import os
from unittest.mock import MagicMock, patch

import pytest
import requests

mod = importlib.import_module("src.lambda.france_travail.ingest_france_travail")
get_ft_access_token = mod.get_ft_access_token
fetch_france_travail_offers = mod.fetch_france_travail_offers
lambda_handler = mod.lambda_handler


class TestGetFTAccessToken:
    def test_successful_token_retrieval(self):
        mock_response = MagicMock()
        mock_response.json.return_value = {"access_token": "abc123"}
        mock_response.raise_for_status = MagicMock()

        with patch("requests.post", return_value=mock_response) as mock_post:
            token = get_ft_access_token("client_id", "client_secret")

        assert token == "abc123"
        args, kwargs = mock_post.call_args
        assert "francetravail.fr" in args[0]
        assert kwargs["data"]["client_id"] == "client_id"
        assert kwargs["data"]["grant_type"] == "client_credentials"

    def test_request_exception_raises(self):
        with patch("requests.post", side_effect=requests.exceptions.RequestException("Auth failed")):
            with pytest.raises(requests.exceptions.RequestException, match="Auth failed"):
                get_ft_access_token("cid", "csecret")


class TestFetchFranceTravailOffers:
    @patch.object(mod, "get_ft_access_token", return_value="token123")
    def test_successful_fetch(self, mock_token):
        mock_response = MagicMock()
        mock_response.json.return_value = {
            "nbResultats": 2,
            "resultats": [
                {"id": "FT1", "intitule": "Data Engineer"},
                {"id": "FT2", "intitule": "Data Analyst"},
            ],
        }
        mock_response.raise_for_status = MagicMock()

        with patch("requests.get", return_value=mock_response) as mock_get:
            result = fetch_france_travail_offers("Data Engineer", 44, "cid", "csecret")

        assert result["count"] == 2
        assert len(result["results"]) == 2
        assert result["keyword"] == "Data Engineer"
        assert "ingested_at" in result
        mock_get.assert_called_once()
        args, kwargs = mock_get.call_args
        assert "Authorization" in kwargs["headers"]
        assert kwargs["headers"]["Authorization"] == "Bearer token123"

    @patch.object(mod, "get_ft_access_token", return_value="token123")
    def test_204_no_content_returns_empty(self, mock_token):
        mock_response = MagicMock()
        mock_response.status_code = 204

        with patch("requests.get", return_value=mock_response):
            result = fetch_france_travail_offers("Data", 44, "cid", "csecret")

        assert result["count"] == 0
        assert result["results"] == []

    @patch.object(mod, "get_ft_access_token", return_value="token123")
    def test_request_exception_raises(self, mock_token):
        with patch("requests.get", side_effect=requests.exceptions.RequestException("FT down")):
            with pytest.raises(requests.exceptions.RequestException, match="FT down"):
                fetch_france_travail_offers("Data", 44, "cid", "csecret")


class TestLambdaHandler:
    def test_successful_invocation(self, mock_s3_client):
        event = {"keyword": "Data Engineer", "departement": 44}
        os.environ["FT_CLIENT_ID"] = "test_cid"
        os.environ["FT_CLIENT_SECRET"] = "test_csecret"
        mod.s3_client = mock_s3_client

        with patch.object(mod, "fetch_france_travail_offers") as mock_fetch:
            mock_fetch.return_value = {
                "count": 1,
                "results": [{"id": "FT1"}],
                "keyword": "Data Engineer",
                "ingested_at": "2025-01-01T00:00:00",
            }
            result = lambda_handler(event, None)

        assert result["statusCode"] == 200
        body = json.loads(result["body"])
        assert body["count"] == 1
        assert body["source"] == "france_travail"
        assert mock_s3_client.put_object.called

    def test_exception_propagates(self, mock_s3_client):
        os.environ["FT_CLIENT_ID"] = "cid"
        os.environ["FT_CLIENT_SECRET"] = "csecret"
        mod.s3_client = mock_s3_client

        with patch.object(
            mod, "fetch_france_travail_offers", side_effect=Exception("Ingestion failed")
        ), pytest.raises(Exception, match="Ingestion failed"):
            lambda_handler({"keyword": "Data", "departement": 44}, None)
