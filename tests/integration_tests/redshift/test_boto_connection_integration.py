from typing import Dict, Generator
import pytest
import os
import pandas as pd
from dotenv import load_dotenv
from rsq_aws.redshift.boto_connection import BotoConnection

@pytest.fixture(scope="session")
def connection_setup() -> Dict[str, str]:
    """Load AWS credentials from environment."""
    load_dotenv()
    
    credentials = {
        "workgroup": os.getenv('WORKGROUP'),
        "database": os.getenv('DATABASE'),
        "region": os.getenv('AWS_S3_REGION_NAME'),
        "table_name": os.getenv('TABLE_NAME')
    }
    
    if not all(credentials.values()):
        pytest.fail("AWS credentials not available in environment")
    return credentials

@pytest.fixture(scope="session")
def connection(connection_setup: Dict[str, str]) -> Generator[BotoConnection, None, None]:
    """Fixture for credentials setup."""
    connection = BotoConnection(
        workgroup=connection_setup["workgroup"],
        database=connection_setup["database"],
        region=connection_setup["region"]
    )
    try:
        yield connection    
    except Exception as e:
        raise pytest.fail(f"Failed to create database connection: {e}")
    finally:
        pass

def test_init(
        connection_setup: Dict[str, str],
        connection: BotoConnection,
    ) -> None:
    """Test initialization of Redshift connection."""
    
    assert connection.workgroup == connection_setup["workgroup"]
    assert connection.database == connection_setup["database"]
    assert connection.region == connection_setup["region"]
    assert connection.client is not None

def test_query_success(
    connection_setup: Dict[str, str],
    connection: BotoConnection,    
) -> None:
    """Test successful execution of a Redshift query."""
    
    sql_query = f"SELECT * FROM {connection_setup['table_name']} limit 5;"

    result = connection.query(sql=sql_query)
    
    assert isinstance(result, pd.DataFrame)
    assert not result.empty
    assert result.shape[0] == 5

def test_query_failure(
    connection: BotoConnection,
) -> None:
    """Test failed execution of a Redshift query."""
    
    sql_query = "SELECT * FROM non_existent_table;"
    
    with pytest.raises(Exception):
        connection.query(sql=sql_query)
    
