from typing import Generator, Dict, Any
import pytest
from unittest.mock import Mock, patch, MagicMock
import pandas as pd
import psycopg2
from src.rsq_aws.redshift.psycopg_connection import PsycopgConnection
from src.rsq_aws.redshift._private._interfaces import RedshiftConnection

@pytest.fixture
def mock_session() -> Generator[MagicMock, None, None]:
    """Fixture for mocked boto3 session."""
    mock = MagicMock()
    mock.client.return_value.get_credentials.return_value = {
        "dbUser": "test-user",
        "dbPassword": "test-password"
    }
    with patch('boto3.Session', return_value=mock):
        yield mock

@pytest.fixture
def psycopg_connection(mock_session: MagicMock) -> Generator[PsycopgConnection, None, None]:
    """Fixture for PsycopgConnection instance."""
    with patch('psycopg2.connect') as mock_connect:
        mock_cursor = MagicMock()
        mock_connection = MagicMock()
        mock_connection.cursor.return_value = mock_cursor
        mock_connect.return_value = mock_connection
        
        connection = PsycopgConnection(
            database="test-db",
            workgroup="test-workgroup",
            host="test-host",
            port=5439,
            region="eu-west-2"
        )
        connection._connect()  # This will now use our mocked connection
        yield connection

def test_protocol_implementation() -> None:
    """Verify PsycopgConnection implements RedshiftConnection protocol."""
    with patch.object(PsycopgConnection, '_connect'):
        connection: RedshiftConnection = PsycopgConnection(
            database="test-db",
            workgroup="test-workgroup",
            host="test-host",
            port=5439,
            region="eu-west-2"
        )
        assert isinstance(connection.workgroup, str)
        assert isinstance(connection.database, str)
        assert isinstance(connection.region, str)

def test_init(psycopg_connection: PsycopgConnection) -> None:
    """Test initialization of PsycopgConnection."""
    assert psycopg_connection.database == "test-db"
    assert psycopg_connection.workgroup == "test-workgroup"
    assert psycopg_connection.host == "test-host"
    assert psycopg_connection.port == 5439
    assert psycopg_connection.region == "eu-west-2"

def test_connect_with_credentials(mock_session: MagicMock) -> None:
    """Test that connection is made with correct credentials."""
    with patch('psycopg2.connect') as mock_connect:
        mock_connection = MagicMock()
        mock_connect.return_value = mock_connection
        
        connection = PsycopgConnection(
            database="test-db",
            workgroup="test-workgroup",
            host="test-host",
            port=5439,
            region="eu-west-2"
        )
        
        connection._connect()
        
        mock_connect.assert_called_once_with(
            dbname="test-db",
            user="test-user",
            password="test-password",
            host="test-host",
            port=5439
        )

def test_query_success(psycopg_connection: PsycopgConnection) -> None:
    """Test successful query execution."""
    test_query = "SELECT * FROM test_table"
    
    # Get the mock cursor from the fixture's connection
    mock_cursor = psycopg_connection.connection.cursor()
    mock_cursor.description = [("col1",), ("col2",)]
    mock_cursor.fetchall.return_value = [("value1", "value2")]
    
    # Execute query
    result = psycopg_connection.query(test_query)
    
    # Verify query execution
    mock_cursor.execute.assert_called_once_with(test_query)
    assert isinstance(result, pd.DataFrame)
    assert list(result.columns) == ["col1", "col2"]
    assert result.iloc[0]["col1"] == "value1"
    assert result.iloc[0]["col2"] == "value2"

def test_query_with_different_data_types(psycopg_connection: PsycopgConnection) -> None:
    """Test query with different data types."""
    # Get the mock cursor from the fixture's connection
    mock_cursor = psycopg_connection.connection.cursor()
    mock_cursor.description = [("int_col",), ("float_col",), ("str_col",), ("null_col",)]
    mock_cursor.fetchall.return_value = [(1, 2.5, "text", None)]
    
    result = psycopg_connection.query("SELECT * FROM test_table")
    
    assert result.iloc[0]["int_col"] == 1
    assert result.iloc[0]["float_col"] == 2.5
    assert result.iloc[0]["str_col"] == "text"
    assert pd.isna(result.iloc[0]["null_col"])

def test_connection_management(psycopg_connection: PsycopgConnection) -> None:
    """Test connection management."""
    # Get the mock connection from the fixture
    mock_connection = psycopg_connection.connection
    
    # Test close
    psycopg_connection.close()
    
    mock_connection.close.assert_called_once()
    assert psycopg_connection.connection is None
    
    # Test is_connected
    assert not psycopg_connection._is_connected()

def test_reconnection(psycopg_connection: PsycopgConnection) -> None:
    """Test automatic reconnection when connection is lost."""
    test_query = "SELECT * FROM test_table"
    
    # Get the mock cursor from the fixture's connection
    mock_cursor = psycopg_connection.connection.cursor()
    mock_cursor.description = [("col1",)]
    mock_cursor.fetchall.return_value = [("value1",)]
    
    # First query
    result1 = psycopg_connection.query(test_query)
    assert result1.iloc[0]["col1"] == "value1"
    
    # Simulate connection loss
    psycopg_connection.connection = None
    
    # Update mock cursor for second query
    mock_cursor.fetchall.return_value = [("value2",)]
    
    # Second query should trigger reconnection
    result2 = psycopg_connection.query(test_query)
    assert result2.iloc[0]["col1"] == "value2"

def test_query_error_handling(psycopg_connection: PsycopgConnection) -> None:
    """Test error handling during query execution."""
    # Get the mock cursor from the fixture's connection
    mock_cursor = psycopg_connection.connection.cursor()
    mock_cursor.execute.side_effect = psycopg2.Error("Test DB Error")
    
    # Verify error is propagated
    with pytest.raises(psycopg2.Error) as exc_info:
        psycopg_connection.query("SELECT * FROM test_table")
    
    assert str(exc_info.value) == "Test DB Error"
