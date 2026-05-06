import importlib
import json
import os
from unittest.mock import MagicMock, patch

import boto3
import pytest
from moto import mock_aws

SOURCE_MODULES = {
    "adzuna": "src.lambda.adzuna.ingest_adzuna",
    "france_travail": "src.lambda.france_travail.ingest_france_travail",
    "jooble": "src.lambda.jooble.ingest_jooble",
    "jsearch": "src.lambda.jsearch.ingest_jsearch",
}


@pytest.fixture(autouse=True)
def aws_credentials():
    os.environ["AWS_ACCESS_KEY_ID"] = "testing"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "testing"
    os.environ["AWS_DEFAULT_REGION"] = "eu-west-3"
    os.environ["AWS_REGION"] = "eu-west-3"
    os.environ["BUCKET_NAME"] = "test-bucket"


@pytest.fixture
def s3_bucket():
    with mock_aws():
        conn = boto3.resource("s3", region_name="eu-west-3")
        conn.create_bucket(
            Bucket="test-bucket",
            CreateBucketConfiguration={"LocationConstraint": "eu-west-3"},
        )
        yield


def _make_mock_response(json_data, status_code=200):
    resp = MagicMock()
    resp.json.return_value = json_data
    resp.status_code = status_code
    resp.raise_for_status = MagicMock()
    return resp


@pytest.mark.integration
class TestLambdaS3Integration:
    def test_adzuna_writes_valid_json_to_s3(self, s3_bucket):
        os.environ["ADZUNA_APP_ID"] = "test_id"
        os.environ["ADZUNA_APP_KEY"] = "test_key"

        mod = importlib.import_module(SOURCE_MODULES["adzuna"])
        page1 = _make_mock_response(
            {"count": 2, "results": [{"id": "1", "title": "DE"}, {"id": "2", "title": "DS"}]}
        )
        page2 = _make_mock_response({"count": 2, "results": []})

        with patch("requests.get", side_effect=[page1, page2]):
            mod.s3_client = boto3.client("s3", region_name="eu-west-3")
            result = mod.lambda_handler({"keyword": "Data Engineer", "where": "Nantes"}, None)

        assert result["statusCode"] == 200
        body = json.loads(result["body"])
        assert body["count"] == 2
        assert "adzuna" in body["file"]

        s3 = boto3.client("s3", region_name="eu-west-3")
        obj = s3.get_object(Bucket="test-bucket", Key=body["file"])
        payload = json.loads(obj["Body"].read())
        assert payload["count"] == 2
        assert len(payload["results"]) == 2
        assert "ingested_at" in payload

    def test_france_travail_writes_valid_json_to_s3(self, s3_bucket):
        os.environ["FT_CLIENT_ID"] = "cid"
        os.environ["FT_CLIENT_SECRET"] = "csecret"

        mod = importlib.import_module(SOURCE_MODULES["france_travail"])
        with patch("requests.post", return_value=_make_mock_response({"access_token": "token123"})), patch(
            "requests.get",
            return_value=_make_mock_response(
                {"nbResultats": 1, "resultats": [{"id": "FT1", "intitule": "DE"}]}
            ),
        ):
            mod.s3_client = boto3.client("s3", region_name="eu-west-3")
            result = mod.lambda_handler({"keyword": "Data", "departement": 44}, None)

        assert result["statusCode"] == 200
        body = json.loads(result["body"])
        assert body["count"] == 1

        s3 = boto3.client("s3", region_name="eu-west-3")
        obj = s3.get_object(Bucket="test-bucket", Key=body["file"])
        payload = json.loads(obj["Body"].read())
        assert payload["count"] == 1
        assert len(payload["results"]) == 1

    def test_s3_key_follows_expected_pattern(self, s3_bucket):
        os.environ["JOOBLE_API_KEY"] = "key"

        mod = importlib.import_module(SOURCE_MODULES["jooble"])
        with patch("requests.post", return_value=_make_mock_response({"totalCount": 1, "jobs": [{"id": "j1"}]})):
            mod.s3_client = boto3.client("s3", region_name="eu-west-3")
            result = mod.lambda_handler({"keyword": "Data Engineer", "where": "Nantes"}, None)

        body = json.loads(result["body"])
        key = body["file"]
        assert key.startswith("jooble/")
        assert "data_engineer" in key
        assert key.endswith(".json")

    def test_lambda_handler_produces_expected_response_structure(self, s3_bucket):
        os.environ["JSEARCH_API_KEY"] = "key"

        mod = importlib.import_module(SOURCE_MODULES["jsearch"])
        with patch("requests.get", return_value=_make_mock_response({"data": [{"job_id": "js1"}]})):
            mod.s3_client = boto3.client("s3", region_name="eu-west-3")
            result = mod.lambda_handler({"keyword": "Data", "where": "Nantes"}, None)

        body = json.loads(result["body"])
        assert "count" in body
        assert "file" in body
        assert isinstance(body["count"], int)

    def test_schema_validation_rejects_jobs_as_non_list(self, s3_bucket):
        os.environ["JOOBLE_API_KEY"] = "key"

        mod = importlib.import_module(SOURCE_MODULES["jooble"])
        malformed = _make_mock_response({"totalCount": 0, "jobs": "not_a_list"})

        with patch("requests.post", return_value=malformed):
            mod.s3_client = boto3.client("s3", region_name="eu-west-3")
            with pytest.raises(ValueError, match="Schéma invalide"):
                mod.lambda_handler({"keyword": "Data", "where": "Nantes"}, None)

    def test_empty_response_returns_success_with_zero_count(self, s3_bucket):
        os.environ["JSEARCH_API_KEY"] = "key"

        mod = importlib.import_module(SOURCE_MODULES["jsearch"])
        with patch("requests.get", return_value=_make_mock_response({"data": []})):
            mod.s3_client = boto3.client("s3", region_name="eu-west-3")
            result = mod.lambda_handler({"keyword": "Data", "where": "Nantes"}, None)

        body = json.loads(result["body"])
        assert body["count"] == 0
