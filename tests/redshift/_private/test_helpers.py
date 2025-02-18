from typing import Generator
import pytest
from unittest.mock import Mock, patch, MagicMock
from src.rsq_aws.redshift._private._helpers import Credentials

@pytest.fixture
def credentials_setup() -> tuple[str, str]:
    """Fixture for credentials setup."""
    return ("test-workgroup", "us-east-1")

@pytest.fixture
def mock_session() -> Generator[MagicMock, None, None]:
    """Fixture for mocked boto3 session."""
    with patch('boto3.Session') as mock:
        mock_client = MagicMock()
        mock.return_value.client.return_value = mock_client
        yield mock

def test_init(credentials_setup: tuple[str, str], mock_session: MagicMock) -> None:
    """Test initialization of Credentials."""
    workgroup, region = credentials_setup
    mock_client = mock_session.return_value.client.return_value

    # Create credentials instance
    creds = Credentials(workgroup, region)

    # Verify session creation
    mock_session.assert_called_once_with(region=region)
    # Verify client creation
    mock_session.return_value.client.assert_called_once_with("redshift-serverless")
    # Verify attributes
    assert creds.workgroup == workgroup
    assert creds.region == region
    assert creds.client == mock_client

def test_get_credentials_success(
    credentials_setup: tuple[str, str],
    mock_session: MagicMock
) -> None:
    """Test successful retrieval of credentials."""
    workgroup, region = credentials_setup
    
    # Setup mock response
    mock_credentials = {
        "dbUser": "test-user",
        "dbPassword": "test-password"
    }
    mock_client = mock_session.return_value.client.return_value
    mock_client.get_credentials.return_value = mock_credentials

    # Create credentials instance and get credentials
    creds = Credentials(workgroup, region)
    result = creds.get()

    # Verify get_credentials was called with correct parameters
    mock_client.get_credentials.assert_called_once_with(
        workgroupName=workgroup
    )
    # Verify returned credentials
    assert result == mock_credentials

def test_get_credentials_error(
    credentials_setup: tuple[str, str],
    mock_session: MagicMock
) -> None:
    """Test error handling when getting credentials fails."""
    workgroup, region = credentials_setup
    
    # Setup mock to raise an exception
    mock_client = mock_session.return_value.client.return_value
    mock_client.get_credentials.side_effect = Exception("Failed to get credentials")

    # Create credentials instance
    creds = Credentials(workgroup, region)

    # Verify exception is propagated
    with pytest.raises(Exception) as exc_info:
        creds.get()
    assert "Failed to get credentials" in str(exc_info.value)

def test_session_creation_error(credentials_setup: tuple[str, str]) -> None:
    """Test error handling when session creation fails."""
    workgroup, region = credentials_setup
    
    # Setup mock to raise an exception
    with patch('boto3.Session', side_effect=Exception("Failed to create session")):
        # Verify exception is propagated
        with pytest.raises(Exception) as exc_info:
            Credentials(workgroup, region)
        assert "Failed to create session" in str(exc_info.value)
