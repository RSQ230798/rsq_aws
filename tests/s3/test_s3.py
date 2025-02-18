from typing import Generator, Dict, Any
import pytest
from unittest.mock import Mock, patch, MagicMock
import pandas as pd
import io
import json
import pyarrow as pa
import pyarrow.parquet as pq
from src.rsq_aws.s3.s3 import S3

@pytest.fixture
def s3_credentials() -> Dict[str, str]:
    """Fixture for S3 credentials."""
    return {
        "bucket": "test-bucket",
        "access_key_id": "test-key",
        "secret_access_key": "test-secret",
        "region_name": "us-east-1"
    }

@pytest.fixture
def mock_boto3_client() -> Generator[Mock, None, None]:
    """Fixture for mocked boto3 client."""
    with patch('boto3.client') as mock:
        yield mock

@pytest.fixture
def s3_client(s3_credentials: Dict[str, str], mock_boto3_client: Mock) -> S3:
    """Fixture for S3 client instance."""
    return S3(**s3_credentials)

def test_init_success(s3_client: S3, s3_credentials: Dict[str, str]) -> None:
    """Test successful initialization of S3 client."""
    assert s3_client.bucket == s3_credentials["bucket"]
    assert s3_client.access_key_id == s3_credentials["access_key_id"]
    assert s3_client.secret_access_key == s3_credentials["secret_access_key"]
    assert s3_client.region_name == s3_credentials["region_name"]

def test_init_failure(s3_credentials: Dict[str, str], mock_boto3_client: Mock) -> None:
    """Test failed initialization of S3 client."""
    mock_boto3_client.side_effect = Exception("Connection failed")
    with pytest.raises(Exception) as exc_info:
        S3(**s3_credentials)
    assert "Failed to connect to S3" in str(exc_info.value)

def test_get_directory_tree(s3_client: S3, mock_boto3_client: Mock) -> None:
    """Test getting directory tree structure."""
    mock_client = mock_boto3_client.return_value
    
    def paginate_side_effect(**kwargs: Any) -> list[Dict[str, Any]]:
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

    tree = s3_client.get_directory_tree("")
    assert "folder1" in tree
    assert "folder2" in tree
    assert "files" in tree["folder1"]
    assert "file1.json" in tree["folder1"]["files"]
    assert "file2.parquet" in tree["folder1"]["files"]

def test_upload_json_object(s3_client: S3, mock_boto3_client: Mock) -> None:
    """Test uploading JSON object."""
    mock_client = mock_boto3_client.return_value
    test_data = {"key": "value"}
    result = s3_client.upload_object(test_data, "test.json")
    
    mock_client.put_object.assert_called_once()
    assert result == "JSON file uploaded successfully!"

def test_upload_parquet_object(s3_client: S3, mock_boto3_client: Mock) -> None:
    """Test uploading Parquet object."""
    mock_client = mock_boto3_client.return_value
    test_df = pd.DataFrame({"col1": [1, 2, 3]})
    result = s3_client.upload_object(test_df, "test.parquet")
    
    mock_client.put_object.assert_called_once()
    assert result == "Parquet file uploaded successfully!"

def test_upload_invalid_file_type(s3_client: S3) -> None:
    """Test uploading invalid file type."""
    with pytest.raises(Exception) as exc_info:
        s3_client.upload_object({}, "test.txt")
    assert "Unsupported file type" in str(exc_info.value)

def test_get_json_object(s3_client: S3, mock_boto3_client: Mock) -> None:
    """Test getting JSON object."""
    mock_client = mock_boto3_client.return_value
    test_data = {"key": "value"}
    mock_client.get_object.return_value = {
        'Body': io.BytesIO(json.dumps(test_data).encode())
    }
    
    result = s3_client.get_object("test.json")
    assert result == test_data

def test_get_parquet_object(s3_client: S3, mock_boto3_client: Mock) -> None:
    """Test getting Parquet object."""
    mock_client = mock_boto3_client.return_value
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
    
    result = s3_client.get_object("test.parquet")
    pd.testing.assert_frame_equal(result, test_df)

def test_delete_object(s3_client: S3, mock_boto3_client: Mock) -> None:
    """Test deleting object."""
    mock_client = mock_boto3_client.return_value
    result = s3_client.delete("test.json")
    
    mock_client.delete_object.assert_called_once_with(
        Bucket="test-bucket",
        Key="test.json"
    )
    assert result == "File deleted successfully!"

def test_copy_object(s3_client: S3, mock_boto3_client: Mock) -> None:
    """Test copying object."""
    mock_client = mock_boto3_client.return_value
    s3_client.copy_object("source.json", "dest.json")
    
    mock_client.copy_object.assert_called_once_with(
        CopySource={'Bucket': 'test-bucket', 'Key': 'source.json'},
        Bucket='test-bucket',
        Key='dest.json'
    )

def test_copy_object_with_delete(s3_client: S3, mock_boto3_client: Mock) -> None:
    """Test copying object with deletion of source."""
    mock_client = mock_boto3_client.return_value
    s3_client.copy_object("source.json", "dest.json", must_delete=True)
    
    mock_client.copy_object.assert_called_once()
    mock_client.delete_object.assert_called_once_with(
        Bucket="test-bucket",
        Key="source.json"
    )

def test_combine_parquet_files(s3_client: S3, mock_boto3_client: Mock) -> None:
    """Test combining Parquet files."""
    mock_client = mock_boto3_client.return_value
    test_df = pd.DataFrame({"col1": [1, 2, 3]})
    
    # Create a proper Parquet file using pyarrow
    table = pa.Table.from_pandas(test_df)
    parquet_buffer = io.BytesIO()
    pq.write_table(table, parquet_buffer)
    parquet_buffer.seek(0)
    parquet_content = parquet_buffer.getvalue()
    
    # Create a new mock response for each file
    def get_object_side_effect(*args: Any, **kwargs: Any) -> Dict[str, Any]:
        return {'Body': io.BytesIO(parquet_content)}
    
    mock_client.get_object.side_effect = get_object_side_effect
    
    files = ["file1.parquet", "file2.parquet"]
    s3_client.combine_parquet_files("combined.parquet", files)
    
    # Verify the mock calls
    assert mock_client.get_object.call_count == len(files)
    assert mock_client.put_object.call_count == 1
    
    # Verify the put_object was called with non-empty content
    put_object_calls = mock_client.put_object.call_args_list
    assert len(put_object_calls[0].kwargs['Body'].getvalue()) > 0
