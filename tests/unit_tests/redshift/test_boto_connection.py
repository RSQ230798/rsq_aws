from typing import Generator, Dict, Any
import pytest
from unittest.mock import Mock, patch
import pandas as pd
from rsq_aws.redshift.boto_connection import BotoConnection
from rsq_aws.redshift._private._interfaces import RedshiftConnection

@pytest.fixture
def mock_boto3_client() -> Generator[Mock, None, None]:
    """Fixture for mocked boto3 client."""
    with patch('boto3.client') as mock_client:
        yield mock_client.return_value

@pytest.fixture
def boto_connection(mock_boto3_client: Mock) -> BotoConnection:
    """Fixture for BotoConnection instance."""
    return BotoConnection(
        workgroup="test-workgroup",
        database="test-db",
        region="us-east-1"
    )

def test_protocol_implementation() -> None:
    """Verify BotoConnection implements RedshiftConnection protocol."""
    connection: RedshiftConnection = BotoConnection(
        workgroup="test-workgroup",
        database="test-db",
        region="us-east-1"
    )
    assert isinstance(connection.workgroup, str)
    assert isinstance(connection.database, str)
    assert isinstance(connection.region, str)

def test_init(boto_connection: BotoConnection) -> None:
    """Test initialization of BotoConnection."""
    assert boto_connection.workgroup == "test-workgroup"
    assert boto_connection.database == "test-db"
    assert boto_connection.region == "us-east-1"

def test_query_success(
    boto_connection: BotoConnection,
    mock_boto3_client: Mock
) -> None:
    """Test successful query execution."""
    # Mock the query execution response
    mock_boto3_client.execute_statement.return_value = {"Id": "query-id"}
    
    # Create a list of responses for describe_statement
    status_responses = [
        {"Status": "SUBMITTED"},
        {"Status": "RUNNING"},
        {"Status": "RUNNING"},
        {"Status": "FINISHED", "HasResultSet": True}
    ]
    
    def describe_statement_side_effect(**kwargs: Any) -> Dict[str, Any]:
        if not status_responses:
            return {"Status": "FINISHED", "HasResultSet": True}
        return status_responses.pop(0)
        
    mock_boto3_client.describe_statement.side_effect = describe_statement_side_effect
    
    # Mock the query results
    mock_boto3_client.get_statement_result.return_value = {
        "ColumnMetadata": [{"name": "col1"}, {"name": "col2"}],
        "Records": [
            [{"stringValue": "value1"}, {"stringValue": "value2"}]
        ]
    }
    
    result = boto_connection.query("SELECT * FROM test_table")
    assert isinstance(result, pd.DataFrame)
    assert list(result.columns) == ["col1", "col2"]
    mock_boto3_client.execute_statement.assert_called_once()

def test_query_failure(
    boto_connection: BotoConnection,
    mock_boto3_client: Mock
) -> None:
    """Test failed query execution."""
    mock_boto3_client.execute_statement.return_value = {"Id": "query-id"}
    mock_boto3_client.describe_statement.return_value = {
        "Status": "FAILED",
        "Error": "Query execution failed"
    }
    
    with pytest.raises(Exception) as exc_info:
        boto_connection.query("SELECT * FROM test_table")
    assert "Query failed" in str(exc_info.value)
    mock_boto3_client.execute_statement.assert_called_once()

def test_query_no_results(
    boto_connection: BotoConnection,
    mock_boto3_client: Mock
) -> None:
    """Test query with no results."""
    mock_boto3_client.execute_statement.return_value = {"Id": "query-id"}
    mock_boto3_client.describe_statement.return_value = {
        "Status": "FINISHED",
        "HasResultSet": False
    }
    
    result = boto_connection.query("INSERT INTO test_table VALUES (1)")
    assert isinstance(result, pd.DataFrame)
    assert result.empty
    mock_boto3_client.execute_statement.assert_called_once()
