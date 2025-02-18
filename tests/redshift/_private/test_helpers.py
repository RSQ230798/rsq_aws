import unittest
from unittest.mock import patch, MagicMock
from src.rsq_aws.redshift._private._helpers import Credentials

class TestCredentials(unittest.TestCase):
    """Test suite for Credentials class."""

    def setUp(self):
        """Set up test fixtures before each test method."""
        self.workgroup = "test-workgroup"
        self.region = "us-east-1"

    @patch('boto3.Session')
    def test_init(self, mock_session):
        """Test initialization of Credentials."""
        # Setup mock
        mock_client = MagicMock()
        mock_session.return_value.client.return_value = mock_client

        # Create credentials instance
        creds = Credentials(self.workgroup, self.region)

        # Verify session creation
        mock_session.assert_called_once_with(region=self.region)
        # Verify client creation
        mock_session.return_value.client.assert_called_once_with("redshift-serverless")
        # Verify attributes
        self.assertEqual(creds.workgroup, self.workgroup)
        self.assertEqual(creds.region, self.region)
        self.assertEqual(creds.client, mock_client)

    @patch('boto3.Session')
    def test_get_credentials_success(self, mock_session):
        """Test successful retrieval of credentials."""
        # Setup mock response
        mock_credentials = {
            "dbUser": "test-user",
            "dbPassword": "test-password"
        }
        mock_client = MagicMock()
        mock_client.get_credentials.return_value = mock_credentials
        mock_session.return_value.client.return_value = mock_client

        # Create credentials instance and get credentials
        creds = Credentials(self.workgroup, self.region)
        result = creds.get()

        # Verify get_credentials was called with correct parameters
        mock_client.get_credentials.assert_called_once_with(
            workgroupName=self.workgroup
        )
        # Verify returned credentials
        self.assertEqual(result, mock_credentials)

    @patch('boto3.Session')
    def test_get_credentials_error(self, mock_session):
        """Test error handling when getting credentials fails."""
        # Setup mock to raise an exception
        mock_client = MagicMock()
        mock_client.get_credentials.side_effect = Exception("Failed to get credentials")
        mock_session.return_value.client.return_value = mock_client

        # Create credentials instance
        creds = Credentials(self.workgroup, self.region)

        # Verify exception is propagated
        with self.assertRaises(Exception) as context:
            creds.get()
        self.assertIn("Failed to get credentials", str(context.exception))

    @patch('boto3.Session')
    def test_session_creation_error(self, mock_session):
        """Test error handling when session creation fails."""
        # Setup mock to raise an exception
        mock_session.side_effect = Exception("Failed to create session")

        # Verify exception is propagated
        with self.assertRaises(Exception) as context:
            Credentials(self.workgroup, self.region)
        self.assertIn("Failed to create session", str(context.exception))


if __name__ == '__main__':
    unittest.main()
