from typing import Dict
import pytest
import os
from dotenv import load_dotenv
from src.rsq_aws.redshift._private._helpers import Credentials

@pytest.fixture(scope="session")
def credentials_setup() -> Dict[str, str]:
    """Load AWS credentials from environment."""
    load_dotenv()
    
    credentials = {
        "workgroup": os.getenv('WORKGROUP'),
        "region": os.getenv('AWS_S3_REGION_NAME')
    }
    
    if not all(credentials.values()):
        pytest.fail("AWS credentials not available in environment")
    return credentials

@pytest.fixture
def credentials(credentials_setup: Dict[str, str]) -> Credentials:
    """Fixture for credentials setup."""
    return Credentials(credentials_setup["workgroup"], credentials_setup["region"])

def test_init(
        credentials_setup: Dict[str, str],
        credentials: Credentials,
    ) -> None:
    """Test initialization of S3."""
    
    assert credentials.workgroup == credentials_setup["workgroup"]
    assert credentials.region == credentials_setup["region"]
    assert credentials.client is not None

def test_get_credentials_success(
    credentials: Credentials
) -> None:
    """Test successful retrieval of credentials."""
    
    result = credentials.get()
    assert result is not None
    assert "dbUser" in result
    assert "dbPassword" in result


