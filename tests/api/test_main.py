from unittest.mock import MagicMock, patch

from fastapi.testclient import TestClient

from api.main import app

client = TestClient(app)


class TestHealthEndpoints:
    def test_read_root(self):
        response = client.get("/")
        assert response.status_code == 200
        data = response.json()
        assert data["message"] == "API JobRadar - Opérationnelle (AWS Lambda)"
        assert "health" in data["endpoints"]
        assert "jobs" in data["endpoints"]

    def test_health_check(self):
        response = client.get("/health")
        assert response.status_code == 200
        data = response.json()
        assert data["status"] == "healthy"
        assert data["service"] == "jobradar-api-serverless"

    @patch("api.main.boto3.client")
    def test_pipeline_health_success(self, mock_boto3):
        mock_athena = MagicMock()
        mock_boto3.return_value = mock_athena

        mock_athena.start_query_execution.return_value = {
            "QueryExecutionId": "query-123"
        }

        mock_athena.get_query_execution.side_effect = [
            {"QueryExecution": {"Status": {"State": "RUNNING"}}},
            {"QueryExecution": {"Status": {"State": "SUCCEEDED"}}},
        ]

        mock_athena.get_query_results.return_value = {
            "ResultSet": {
                "Rows": [
                    {"Data": [{"VarCharValue": "status"}, {"VarCharValue": "last_run"}, {"VarCharValue": "count"}]},
                    {"Data": [{"VarCharValue": "SUCCESS"}, {"VarCharValue": "2025-01-01"}, {"VarCharValue": "42"}]},
                ]
            }
        }

        response = client.get("/health/pipeline")
        assert response.status_code == 200
        data = response.json()
        assert data["status"] == "SUCCESS"
        assert data["count"] == 42

    @patch("api.main.boto3.client")
    def test_pipeline_health_query_failed(self, mock_boto3):
        mock_athena = MagicMock()
        mock_boto3.return_value = mock_athena
        mock_athena.start_query_execution.return_value = {
            "QueryExecutionId": "query-456"
        }
        mock_athena.get_query_execution.return_value = {
            "QueryExecution": {"Status": {"State": "FAILED", "StateChangeReason": "Syntax error"}}
        }

        response = client.get("/health/pipeline")
        assert response.status_code == 500

    @patch("api.main.boto3.client")
    def test_pipeline_health_no_results(self, mock_boto3):
        mock_athena = MagicMock()
        mock_boto3.return_value = mock_athena
        mock_athena.start_query_execution.return_value = {
            "QueryExecutionId": "query-789"
        }
        mock_athena.get_query_execution.return_value = {
            "QueryExecution": {"Status": {"State": "SUCCEEDED"}}
        }
        mock_athena.get_query_results.return_value = {
            "ResultSet": {"Rows": [{"Data": [{"VarCharValue": "col1"}]}]}
        }

        response = client.get("/health/pipeline")
        assert response.status_code == 200
        data = response.json()
        assert data["status"] == "UNKNOWN"


class TestJobsEndpoint:
    def test_jobs_missing_api_key(self):
        response = client.get("/jobs")
        assert response.status_code == 403
        assert "Clé API" in response.json()["detail"]

    def test_jobs_invalid_api_key(self):
        response = client.get("/jobs", headers={"X-API-Key": "wrong_key"})
        assert response.status_code == 403
        assert "Clé API" in response.json()["detail"]

    @patch("api.main.S3_STAGING", "s3://test/")
    @patch("api.main.connect")
    def test_jobs_success(self, mock_connect, monkeypatch):
        monkeypatch.setenv("INTERNAL_API_KEY", "test_key")

        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_connect.return_value = mock_conn
        mock_conn.cursor.return_value = mock_cursor
        mock_cursor.fetchall.return_value = [
            {
                "title": "Data Engineer",
                "company_name": "Test Corp",
                "city": "Nantes",
                "matching_score": 85,
            }
        ]

        response = client.get("/jobs", headers={"X-API-Key": "test_key"})
        assert response.status_code == 200
        data = response.json()
        assert data["total_count"] == 1
        assert data["jobs"][0]["title"] == "Data Engineer"

    @patch("api.main.S3_STAGING", None)
    def test_jobs_missing_s3_staging(self, monkeypatch):
        monkeypatch.setenv("INTERNAL_API_KEY", "test_key")

        response = client.get("/jobs", headers={"X-API-Key": "test_key"})
        assert response.status_code == 500
        assert "Configuration S3 Staging manquante" in response.json()["detail"]

    @patch("api.main.S3_STAGING", "s3://test/")
    @patch("api.main.connect")
    def test_jobs_query_exception(self, mock_connect, monkeypatch):
        monkeypatch.setenv("INTERNAL_API_KEY", "test_key")

        mock_conn = MagicMock()
        mock_connect.return_value = mock_conn
        mock_conn.cursor.side_effect = Exception("Athena timeout")

        response = client.get("/jobs", headers={"X-API-Key": "test_key"})
        assert response.status_code == 500
