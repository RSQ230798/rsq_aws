import unittest
from unittest.mock import Mock, patch, MagicMock
import pandas as pd
import psycopg2
from src.rsq_aws.redshift.psycopg_connection import PsycopgConnection

class TestPsycopg2Connection(unittest.TestCase):
    """Test suite for Psycopg2Connection class."""

    def setUp(self):
        """Set up test fixtures before each test method."""
        # Mock boto3 session and client
        self.mock_session = MagicMock()
        self.mock_client = MagicMock()
        self.mock_session.client.return_value = self.mock_client
        self.mock_client.get_credentials.return_value = {
            "dbUser": "test-user",
            "dbPassword": "test-password"
        }
        
        # Start session patcher
        self.session_patcher = patch('boto3.Session', return_value=self.mock_session)
        self.session_patcher.start()
        self.addCleanup(self.session_patcher.stop)
        
        # Create the client without connecting
        with patch.object(PsycopgConnection, '_connect'):  # Prevent connection in __init__
            self.psycopg2_client = PsycopgConnection(
                database="test-db",
                workgroup="test-workgroup",
                host="test-host",
                port=5439,
                region="eu-west-2"
            )

    def test_init(self):
        """Test initialization of Psycopg2Connection."""
        self.assertEqual(self.psycopg2_client.database, "test-db")
        self.assertEqual(self.psycopg2_client.workgroup, "test-workgroup")
        self.assertEqual(self.psycopg2_client.host, "test-host")
        self.assertEqual(self.psycopg2_client.port, 5439)
        self.assertEqual(self.psycopg2_client.region, "eu-west-2")

    @patch('psycopg2.connect')
    def test_connect_with_credentials(self, mock_connect):
        """Test that connection is made with correct credentials."""
        # Mock connection
        mock_connection = MagicMock()
        mock_connect.return_value = mock_connection
        
        # Connect
        self.psycopg2_client._connect()
        
        mock_connect.assert_called_once_with(
            dbname="test-db",
            user="test-user",
            password="test-password",
            host="test-host",
            port=5439
        )

    @patch('psycopg2.connect')
    def test_query_success(self, mock_connect):
        """Test successful query execution."""
        test_query = "SELECT * FROM test_table"
        
        # Mock cursor and its methods
        mock_cursor = MagicMock()
        mock_cursor.description = [("col1",), ("col2",)]
        mock_cursor.fetchall.return_value = [("value1", "value2")]
        
        # Mock connection
        mock_connection = MagicMock()
        mock_connection.cursor.return_value = mock_cursor
        mock_connect.return_value = mock_connection
        
        # Execute query
        result = self.psycopg2_client.query(test_query)
        
        # Verify query execution
        mock_cursor.execute.assert_called_once_with(test_query)
        self.assertIsInstance(result, pd.DataFrame)
        self.assertEqual(list(result.columns), ["col1", "col2"])
        self.assertEqual(result.iloc[0]["col1"], "value1")
        self.assertEqual(result.iloc[0]["col2"], "value2")

    @patch('psycopg2.connect')
    def test_query_with_different_data_types(self, mock_connect):
        """Test query with different data types."""
        mock_cursor = MagicMock()
        mock_cursor.description = [("int_col",), ("float_col",), ("str_col",), ("null_col",)]
        mock_cursor.fetchall.return_value = [(1, 2.5, "text", None)]
        
        mock_connection = MagicMock()
        mock_connection.cursor.return_value = mock_cursor
        mock_connect.return_value = mock_connection
        
        result = self.psycopg2_client.query("SELECT * FROM test_table")
        
        self.assertEqual(result.iloc[0]["int_col"], 1)
        self.assertEqual(result.iloc[0]["float_col"], 2.5)
        self.assertEqual(result.iloc[0]["str_col"], "text")
        self.assertTrue(pd.isna(result.iloc[0]["null_col"]))

    @patch('psycopg2.connect')
    def test_connection_management(self, mock_connect):
        """Test connection management."""
        # Mock connection
        mock_connection = MagicMock()
        mock_connect.return_value = mock_connection
        
        # Test close
        self.psycopg2_client.connection = mock_connection
        self.psycopg2_client.close()
        
        mock_connection.close.assert_called_once()
        self.assertIsNone(self.psycopg2_client.connection)
        
        # Test is_connected
        self.assertFalse(self.psycopg2_client._is_connected())

    @patch('psycopg2.connect')
    def test_reconnection(self, mock_connect):
        """Test automatic reconnection when connection is lost."""
        test_query = "SELECT * FROM test_table"
        
        # First connection setup
        mock_cursor1 = MagicMock()
        mock_cursor1.description = [("col1",)]
        mock_cursor1.fetchall.return_value = [("value1",)]
        mock_connection1 = MagicMock()
        mock_connection1.cursor.return_value = mock_cursor1
        
        # Second connection setup (after reconnect)
        mock_cursor2 = MagicMock()
        mock_cursor2.description = [("col1",)]
        mock_cursor2.fetchall.return_value = [("value2",)]
        mock_connection2 = MagicMock()
        mock_connection2.cursor.return_value = mock_cursor2
        
        mock_connect.side_effect = [mock_connection1, mock_connection2]
        
        # First query
        self.psycopg2_client._connect()
        result1 = self.psycopg2_client.query(test_query)
        self.assertEqual(result1.iloc[0]["col1"], "value1")
        
        # Simulate connection loss
        self.psycopg2_client.connection = None
        
        # Second query should trigger reconnection
        result2 = self.psycopg2_client.query(test_query)
        self.assertEqual(result2.iloc[0]["col1"], "value2")
        
        # Verify connect was called twice
        self.assertEqual(mock_connect.call_count, 2)

    def test_query_error_handling(self):
        """Test error handling during query execution."""
        with patch('psycopg2.connect') as mock_connect:
            # Mock cursor that raises an error
            mock_cursor = MagicMock()
            mock_cursor.execute.side_effect = psycopg2.Error("Test DB Error")
            
            mock_connection = MagicMock()
            mock_connection.cursor.return_value = mock_cursor
            mock_connect.return_value = mock_connection
            
            # Verify error is propagated
            with self.assertRaises(psycopg2.Error) as context:
                self.psycopg2_client.query("SELECT * FROM test_table")
            
            self.assertEqual(str(context.exception), "Test DB Error")

if __name__ == '__main__':
    unittest.main()
