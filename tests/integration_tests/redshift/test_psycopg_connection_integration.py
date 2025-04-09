from typing import Dict, Generator
import pytest
import os
import pandas as pd
from dotenv import load_dotenv
from rsq_aws.redshift.psycopg_connection import PsycopgConnection

@pytest.fixture(scope="session")
def connection_setup() -> Dict[str, str]:
    """Load AWS credentials from environment."""
    load_dotenv()
    
    credentials = {
        "workgroup": os.getenv('WORKGROUP'),
        "database": os.getenv('DATABASE'),
        "region": os.getenv('AWS_S3_REGION_NAME'),
        "host": os.getenv('HOST'),
        "port": int(os.getenv('PORT')),
        "table_name": os.getenv('TABLE_NAME')
    }
    
    if not all(credentials.values()):
        pytest.warns("AWS credentials for PsycopgConnection not all available in environment")
    return credentials

@pytest.fixture(scope="session")
def connection(connection_setup: Dict[str, str]) -> Generator[PsycopgConnection, None, None]:
    """Fixture for credentials setup."""
    connection = PsycopgConnection(
            workgroup=connection_setup["workgroup"],
            database=connection_setup["database"],
            region=connection_setup["region"],
            host=connection_setup["host"],
            port=connection_setup["port"]
        )
    try:
        yield connection

    except Exception as e:
        raise pytest.fail(f"Failed to create database connection: {e}")
    
    finally:
        connection.close()
    
def test_init(
        connection_setup: Dict[str, str],
        connection: PsycopgConnection,
    ) -> None:
    """Test initialization of Redshift connection."""
    
    assert connection.workgroup == connection_setup["workgroup"]
    assert connection.database == connection_setup["database"]
    assert connection.region == connection_setup["region"]
    assert connection.host == connection_setup["host"]
    assert connection.port == connection_setup["port"]
    assert connection.connection is None

def test_connection(
    connection: PsycopgConnection,
) -> None:
    """Test connection to Redshift."""
    
    connection._connect()    
    assert connection.connection is not None

def test_close(
    connection: PsycopgConnection,
) -> None:
    """Test closing of a Redshift connection."""
    
    connection.close()
    assert connection.connection is None

def test_query_success(
    connection_setup: Dict[str, str],
    connection: PsycopgConnection,    
) -> None:
    """Test successful execution of a Redshift query."""
    
    sql_query = f"SELECT * FROM {connection_setup['table_name']} limit 5;"

    result = connection.query(sql=sql_query)
    
    assert isinstance(result, pd.DataFrame)
    assert not result.empty
    assert result.shape[0] == 5

def test_query_failure(
    connection: PsycopgConnection,
) -> None:
    """Test failed execution of a Redshift query."""
    
    sql_query = "SELECT * FROM non_existent_table limit 5;"

    with pytest.raises(Exception):
        connection.query(sql=sql_query)
