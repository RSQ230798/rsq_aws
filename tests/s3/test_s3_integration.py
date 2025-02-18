import unittest
import os
import json
import pandas as pd
import uuid
from datetime import datetime
from dotenv import load_dotenv

from src.rsq_aws.s3.s3 import S3

class S3IntegrationTestCase(unittest.TestCase):
    """Base class for S3 integration tests."""

    @classmethod
    def setUpClass(cls):
        """Set up test environment and AWS credentials."""
        load_dotenv()
        
        # Load AWS credentials from environment
        cls.aws_access_key_id = os.getenv('AWS_ACCESS_KEY_ID')
        cls.aws_secret_access_key = os.getenv('AWS_SECRET_ACCESS_KEY')
        cls.bucket = os.getenv('AWS_S3_BUCKET_DATA')
        cls.region = os.getenv('AWS_S3_REGION_NAME')

        # Skip tests if credentials are not available
        if not all([cls.aws_access_key_id, cls.aws_secret_access_key, 
                   cls.bucket, cls.region]):
            raise unittest.SkipTest("AWS credentials not available in environment")

        # Create unique test directory
        cls.test_dir = f"test_integration/{datetime.now().strftime('%Y%m%d_%H%M%S')}_{uuid.uuid4().hex[:8]}/"
        
        # Initialize S3 client
        cls.s3_client = S3(
            bucket=cls.bucket,
            access_key_id=cls.aws_access_key_id,
            secret_access_key=cls.aws_secret_access_key,
            region_name=cls.region
        )

    def setUp(self):
        """Set up test-specific resources."""
        self.test_files = []

    def tearDown(self):
        """Clean up test files."""
        for file_path in self.test_files:
            try:
                self.s3_client.delete(f"{self.test_dir}{file_path}")
            except Exception:
                pass  # Ignore errors during cleanup

    def test_json_upload_download(self):
        """Test uploading and downloading JSON files."""
        # Test data
        test_data = {
            "test_id": str(uuid.uuid4()),
            "timestamp": datetime.now().isoformat(),
            "data": {
                "key1": "value1",
                "key2": 123,
                "key3": ["a", "b", "c"]
            }
        }
        
        # Test file path
        file_path = f"test_file_{uuid.uuid4().hex[:8]}.json"
        self.test_files.append(file_path)
        full_path = f"{self.test_dir}{file_path}"

        # Upload
        upload_result = self.s3_client.upload_object(test_data, full_path)
        self.assertEqual(upload_result, "JSON file uploaded successfully!")

        # Download and verify
        downloaded_data = self.s3_client.get_object(full_path)
        self.assertEqual(downloaded_data, test_data)

    def test_parquet_upload_download(self):
        """Test uploading and downloading Parquet files."""
        # Test data
        test_df = pd.DataFrame({
            'id': range(1000),
            'value': [f"value_{i}" for i in range(1000)],
            'timestamp': pd.date_range(start='2024-01-01', periods=1000)
        })
        
        # Test file path
        file_path = f"test_file_{uuid.uuid4().hex[:8]}.parquet"
        self.test_files.append(file_path)
        full_path = f"{self.test_dir}{file_path}"

        # Upload
        upload_result = self.s3_client.upload_object(test_df, full_path)
        self.assertEqual(upload_result, "Parquet file uploaded successfully!")

        # Download and verify
        downloaded_df = self.s3_client.get_object(full_path)
        pd.testing.assert_frame_equal(downloaded_df, test_df)

    def test_directory_operations(self):
        """Test directory listing and tree structure."""
        # Create test files
        files = [
            f"file1_{uuid.uuid4().hex[:8]}.json",
            f"file2_{uuid.uuid4().hex[:8]}.parquet",
            f"subfolder/file3_{uuid.uuid4().hex[:8]}.json"
        ]
        
        # Upload test files
        for file_path in files:
            full_path = f"{self.test_dir}{file_path}"
            if file_path.endswith('.json'):
                self.s3_client.upload_object({"test": "data"}, full_path)
            else:
                self.s3_client.upload_object(pd.DataFrame({'test': [1, 2, 3]}), full_path)
            self.test_files.append(file_path)

        # Get directory tree
        tree = self.s3_client.get_directory_tree(self.test_dir)
        
        # Verify structure
        self.assertIn('files', tree)
        self.assertIn('subfolder', tree)
        self.assertEqual(len(tree['files']), 2)  # Two files in root
        self.assertIn('files', tree['subfolder'])  # One file in subfolder
        self.assertEqual(len(tree['subfolder']['files']), 1)

    def test_copy_operations(self):
        """Test file copy operations."""
        # Create source file
        source_file = f"source_{uuid.uuid4().hex[:8]}.json"
        dest_file = f"dest_{uuid.uuid4().hex[:8]}.json"
        self.test_files.extend([source_file, dest_file])
        
        source_path = f"{self.test_dir}{source_file}"
        dest_path = f"{self.test_dir}{dest_file}"
        
        # Upload source file
        test_data = {"test": "copy_data"}
        self.s3_client.upload_object(test_data, source_path)
        
        # Copy file
        self.s3_client.copy_object(source_path, dest_path)
        
        # Verify copy
        copied_data = self.s3_client.get_object(dest_path)
        self.assertEqual(copied_data, test_data)

    def test_combine_parquet_files(self):
        """Test combining multiple Parquet files."""
        # Create test files
        dfs = [
            pd.DataFrame({'id': range(i*100, (i+1)*100), 'value': [f"batch_{i}"] * 100})
            for i in range(3)
        ]
        
        files = []
        for i, df in enumerate(dfs):
            file_path = f"part_{i}_{uuid.uuid4().hex[:8]}.parquet"
            full_path = f"{self.test_dir}{file_path}"
            self.s3_client.upload_object(df, full_path)
            files.append(full_path)
            self.test_files.append(file_path)

        # Combined file
        combined_file = f"combined_{uuid.uuid4().hex[:8]}.parquet"
        combined_path = f"{self.test_dir}{combined_file}"
        self.test_files.append(combined_file)

        # Combine files
        self.s3_client.combine_parquet_files(combined_path, files)

        # Verify combined file
        combined_df = self.s3_client.get_object(combined_path)
        self.assertEqual(len(combined_df), 300)  # Total rows from all files
        self.assertTrue(all(col in combined_df.columns for col in ['id', 'value']))

    def test_error_scenarios(self):
        """Test error handling scenarios."""
        # Test invalid file type
        with self.assertRaises(Exception) as context:
            self.s3_client.upload_object({"test": "data"}, f"{self.test_dir}test.txt")
        self.assertIn("Unsupported file type", str(context.exception))

        # Test non-existent file
        with self.assertRaises(Exception) as context:
            self.s3_client.get_object(f"{self.test_dir}nonexistent.json")
        self.assertIn("Failed to get", str(context.exception))

        # Test invalid JSON data
        invalid_json = object()  # Cannot be serialized to JSON
        with self.assertRaises(Exception) as context:
            self.s3_client.upload_object(invalid_json, f"{self.test_dir}test.json")
        self.assertIn("Failed to upload", str(context.exception))


if __name__ == '__main__':
    unittest.main()
