import unittest
from unittest.mock import Mock, patch, MagicMock
import pandas as pd
from src.rsq_aws.redshift.boto_connection import BotoConnection

class TestRedshift(unittest.TestCase):
    """Test suite for Redshift class."""

    def setUp(self):
        """Set up test fixtures before each test method."""
        self.patcher = patch('boto3.client')
        self.mock_boto3_client = self.patcher.start()
        self.redshift_client = BotoConnection(
            workgroup="test-workgroup",
            database="test-db",
            region="us-east-1"
        )

    def tearDown(self):
        """Clean up test fixtures after each test method."""
        self.patcher.stop()

    def test_init(self):
        """Test initialization of Redshift client."""
        self.assertEqual(self.redshift_client.workgroup, "test-workgroup")
        self.assertEqual(self.redshift_client.database, "test-db")
        self.assertEqual(self.redshift_client.region, "us-east-1")

    def test_query_success(self):
        """Test successful query execution."""
        mock_client = self.mock_boto3_client.return_value
        
        # Mock the query execution response
        mock_client.execute_statement.return_value = {"Id": "query-id"}
        
        # Create a list of responses for describe_statement
        status_responses = [
            {"Status": "SUBMITTED"},
            {"Status": "RUNNING"},
            {"Status": "RUNNING"},
            {"Status": "FINISHED", "HasResultSet": True}
        ]
        
        def describe_statement_side_effect(**kwargs):
            if not status_responses:
                return {"Status": "FINISHED", "HasResultSet": True}
            return status_responses.pop(0)
            
        mock_client.describe_statement.side_effect = describe_statement_side_effect
        
        # Mock the query results
        mock_client.get_statement_result.return_value = {
            "ColumnMetadata": [{"name": "col1"}, {"name": "col2"}],
            "Records": [
                [{"stringValue": "value1"}, {"stringValue": "value2"}]
            ]
        }
        
        result = self.redshift_client.query("SELECT * FROM test_table")
        self.assertIsInstance(result, pd.DataFrame)
        self.assertEqual(list(result.columns), ["col1", "col2"])

    def test_query_failure(self):
        """Test failed query execution."""
        mock_client = self.mock_boto3_client.return_value
        mock_client.execute_statement.return_value = {"Id": "query-id"}
        mock_client.describe_statement.return_value = {
            "Status": "FAILED",
            "Error": "Query execution failed"
        }
        
        with self.assertRaises(Exception) as context:
            self.redshift_client.query("SELECT * FROM test_table")
        self.assertIn("Query failed", str(context.exception))

    def test_query_no_results(self):
        """Test query with no results."""
        mock_client = self.mock_boto3_client.return_value
        mock_client.execute_statement.return_value = {"Id": "query-id"}
        mock_client.describe_statement.return_value = {
            "Status": "FINISHED",
            "HasResultSet": False
        }
        
        result = self.redshift_client.query("INSERT INTO test_table VALUES (1)")
        self.assertIsInstance(result, pd.DataFrame)
        self.assertTrue(result.empty)

if __name__ == '__main__':
    unittest.main()
