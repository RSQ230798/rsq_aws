import unittest
from unittest.mock import Mock, patch
import pandas as pd
import io
import json
import pyarrow as pa
import pyarrow.parquet as pq
from src.rsq_aws.s3.core.s3 import S3

class TestS3(unittest.TestCase):
    """Test suite for S3 class."""

    def setUp(self):
        """Set up test fixtures before each test method."""
        self.patcher = patch('boto3.client')
        self.mock_boto3_client = self.patcher.start()
        self.s3_client = S3(
            bucket="test-bucket",
            access_key_id="test-key",
            secret_access_key="test-secret",
            region_name="us-east-1"
        )

    def tearDown(self):
        """Clean up test fixtures after each test method."""
        self.patcher.stop()

    def test_init_success(self):
        """Test successful initialization of S3 client."""
        self.assertEqual(self.s3_client.bucket, "test-bucket")
        self.assertEqual(self.s3_client.access_key_id, "test-key")
        self.assertEqual(self.s3_client.secret_access_key, "test-secret")
        self.assertEqual(self.s3_client.region_name, "us-east-1")

    def test_init_failure(self):
        """Test failed initialization of S3 client."""
        self.mock_boto3_client.side_effect = Exception("Connection failed")
        with self.assertRaises(Exception) as context:
            S3("test-bucket", "test-key", "test-secret", "us-east-1")
        self.assertIn("Failed to connect to S3", str(context.exception))

    def test_get_directory_tree(self):
        """Test getting directory tree structure."""
        mock_client = self.mock_boto3_client.return_value
        
        # Mock the paginator
        def paginate_side_effect(**kwargs):
            prefix = kwargs.get('Prefix', '')
            # Normalize the prefix to handle both with and without trailing slash
            prefix = prefix.rstrip('/')
            if prefix in ('', '/'):
                return [{
                    'Contents': [
                        {'Key': 'folder1/'},
                        {'Key': 'folder1/file1.json'},
                        {'Key': 'folder1/file2.parquet'},
                        {'Key': 'folder2/'},
                        {'Key': 'folder2/subfolder/file3.json'}
                    ],
                    'IsTruncated': False
                }]
            elif prefix == 'folder1':
                return [{
                    'Contents': [
                        {'Key': 'folder1/file1.json'},
                        {'Key': 'folder1/file2.parquet'}
                    ],
                    'IsTruncated': False
                }]
            elif prefix == 'folder2':
                return [{
                    'Contents': [
                        {'Key': 'folder2/subfolder/'},
                        {'Key': 'folder2/subfolder/file3.json'}
                    ],
                    'IsTruncated': False
                }]
            elif prefix == 'folder2/subfolder':
                return [{
                    'Contents': [
                        {'Key': 'folder2/subfolder/file3.json'}
                    ],
                    'IsTruncated': False
                }]
            else:
                return [{'Contents': [], 'IsTruncated': False}]
        
        paginator_mock = Mock()
        paginator_mock.paginate = Mock(side_effect=paginate_side_effect)
        mock_client.get_paginator.return_value = paginator_mock

        tree = self.s3_client.get_directory_tree("")
        self.assertIn("folder1", tree)
        self.assertIn("folder2", tree)
        self.assertIn("files", tree["folder1"])
        self.assertIn("file1.json", tree["folder1"]["files"])
        self.assertIn("file2.parquet", tree["folder1"]["files"])

    def test_upload_json_object(self):
        """Test uploading JSON object."""
        mock_client = self.mock_boto3_client.return_value
        test_data = {"key": "value"}
        result = self.s3_client.upload_object(test_data, "test.json")
        
        mock_client.put_object.assert_called_once()
        self.assertEqual(result, "JSON file uploaded successfully!")

    def test_upload_parquet_object(self):
        """Test uploading Parquet object."""
        mock_client = self.mock_boto3_client.return_value
        test_df = pd.DataFrame({"col1": [1, 2, 3]})
        result = self.s3_client.upload_object(test_df, "test.parquet")
        
        mock_client.put_object.assert_called_once()
        self.assertEqual(result, "Parquet file uploaded successfully!")

    def test_upload_invalid_file_type(self):
        """Test uploading invalid file type."""
        with self.assertRaises(Exception) as context:
            self.s3_client.upload_object({}, "test.txt")
        self.assertIn("Unsupported file type", str(context.exception))

    def test_get_json_object(self):
        """Test getting JSON object."""
        mock_client = self.mock_boto3_client.return_value
        test_data = {"key": "value"}
        mock_client.get_object.return_value = {
            'Body': io.BytesIO(json.dumps(test_data).encode())
        }
        
        result = self.s3_client.get_object("test.json")
        self.assertEqual(result, test_data)

    def test_get_parquet_object(self):
        """Test getting Parquet object."""
        mock_client = self.mock_boto3_client.return_value
        # Create test DataFrame
        test_df = pd.DataFrame({"col1": [1, 2, 3]})
        
        # Create proper Parquet buffer
        parquet_buffer = io.BytesIO()
        test_df.to_parquet(parquet_buffer, engine='pyarrow')
        parquet_buffer.seek(0)
        
        # Mock get_object response with non-empty Parquet file
        mock_client.get_object.return_value = {
            'Body': io.BytesIO(parquet_buffer.getvalue())
        }
        
        result = self.s3_client.get_object("test.parquet")
        pd.testing.assert_frame_equal(result, test_df)

    def test_delete_object(self):
        """Test deleting object."""
        mock_client = self.mock_boto3_client.return_value
        result = self.s3_client.delete("test.json")
        
        mock_client.delete_object.assert_called_once_with(
            Bucket="test-bucket",
            Key="test.json"
        )
        self.assertEqual(result, "File deleted successfully!")

    def test_copy_object(self):
        """Test copying object."""
        mock_client = self.mock_boto3_client.return_value
        self.s3_client.copy_object("source.json", "dest.json")
        
        mock_client.copy_object.assert_called_once_with(
            CopySource={'Bucket': 'test-bucket', 'Key': 'source.json'},
            Bucket='test-bucket',
            Key='dest.json'
        )

    def test_copy_object_with_delete(self):
        """Test copying object with deletion of source."""
        mock_client = self.mock_boto3_client.return_value
        self.s3_client.copy_object("source.json", "dest.json", must_delete=True)
        
        mock_client.copy_object.assert_called_once()
        mock_client.delete_object.assert_called_once_with(
            Bucket="test-bucket",
            Key="source.json"
        )

    def test_combine_parquet_files(self):
        """Test combining Parquet files."""
        mock_client = self.mock_boto3_client.return_value
        test_df = pd.DataFrame({"col1": [1, 2, 3]})
        
        # Create a proper Parquet file using pyarrow
        table = pa.Table.from_pandas(test_df)
        parquet_buffer = io.BytesIO()
        pq.write_table(table, parquet_buffer)
        parquet_buffer.seek(0)
        parquet_content = parquet_buffer.getvalue()
        
        # Create a new mock response for each file
        def get_object_side_effect(*args, **kwargs):
            return {'Body': io.BytesIO(parquet_content)}
        
        mock_client.get_object.side_effect = get_object_side_effect
        
        files = ["file1.parquet", "file2.parquet"]
        self.s3_client.combine_parquet_files("combined.parquet", files)
        
        # Verify the mock calls
        self.assertEqual(mock_client.get_object.call_count, len(files))
        self.assertEqual(mock_client.put_object.call_count, 1)
        
        # Verify the put_object was called with non-empty content
        put_object_calls = mock_client.put_object.call_args_list
        self.assertTrue(len(put_object_calls[0].kwargs['Body'].getvalue()) > 0)


if __name__ == '__main__':
    unittest.main()
