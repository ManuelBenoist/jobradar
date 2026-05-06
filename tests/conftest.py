import os
from unittest.mock import MagicMock, patch

import pytest


@pytest.fixture(autouse=True)
def reset_environ():
    """Ensure clean environment variables for every test."""
    env_backup = dict(os.environ)
    os.environ.setdefault("BUCKET_NAME", "test-bucket")
    os.environ.setdefault("AWS_REGION", "eu-west-3")
    yield
    os.environ.clear()
    os.environ.update(env_backup)


@pytest.fixture
def mock_s3_client():
    with patch("boto3.client") as mock:
        s3_mock = MagicMock()
        mock.return_value = s3_mock
        yield s3_mock


@pytest.fixture
def sample_event():
    return {"keyword": "Data Engineer", "where": "Nantes"}


@pytest.fixture
def sample_event_ft():
    return {"keyword": "Data Engineer", "departement": 44}
