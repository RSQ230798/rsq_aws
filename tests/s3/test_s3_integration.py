from typing import Optional, ClassVar, Generator
import pytest
import os
import json
import pandas as pd
import uuid
from datetime import datetime
from dotenv import load_dotenv

from src.rsq_aws.s3.s3 import S3

@pytest.fixture(scope="session")
def aws_credentials() -> dict[str, str]:
    """Load AWS credentials from environment."""
    load_dotenv()
    
    credentials = {
        "aws_access_key_id": os.getenv('AWS_ACCESS_KEY_ID'),
        "aws_secret_access_key": os.getenv('AWS_SECRET_ACCESS_KEY'),
        "bucket": os.getenv('AWS_S3_BUCKET_DATA'),
        "region": os.getenv('AWS_S3_REGION_NAME')
    }
    
    if not all(credentials.values()):
        pytest.skip("AWS credentials not available in environment")
    
    return credentials

@pytest.fixture(scope="session")
def test_dir() -> str:
    """Create unique test directory name."""
    return f"test_integration/{datetime.now().strftime('%Y%m%d_%H%M%S')}_{uuid.uuid4().hex[:8]}/"

@pytest.fixture
def s3_client(aws_credentials: dict[str, str]) -> S3:
    """Initialize S3 client."""
    return S3(
        bucket=aws_credentials["bucket"],
        access_key_id=aws_credentials["aws_access_key_id"],
        secret_access_key=aws_credentials["aws_secret_access_key"],
        region_name=aws_credentials["region"]
    )

@pytest.fixture
def test_files() -> Generator[list[str], None, None]:
    """Fixture for managing test files."""
    files: list[str] = []
    yield files

def test_json_upload_download(
    s3_client: S3,
    test_dir: str,
    test_files: list[str]
) -> None:
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
    test_files.append(file_path)
    full_path = f"{test_dir}{file_path}"

    # Upload
    upload_result = s3_client.upload_object(test_data, full_path)
    assert upload_result == "JSON file uploaded successfully!"

    # Download and verify
    downloaded_data = s3_client.get_object(full_path)
    assert downloaded_data == test_data

def test_parquet_upload_download(
    s3_client: S3,
    test_dir: str,
    test_files: list[str]
) -> None:
    """Test uploading and downloading Parquet files."""
    # Test data
    test_df = pd.DataFrame({
        'id': range(1000),
        'value': [f"value_{i}" for i in range(1000)],
        'timestamp': pd.date_range(start='2024-01-01', periods=1000)
    })
    
    # Test file path
    file_path = f"test_file_{uuid.uuid4().hex[:8]}.parquet"
    test_files.append(file_path)
    full_path = f"{test_dir}{file_path}"

    # Upload
    upload_result = s3_client.upload_object(test_df, full_path)
    assert upload_result == "Parquet file uploaded successfully!"

    # Download and verify
    downloaded_df = s3_client.get_object(full_path)
    pd.testing.assert_frame_equal(downloaded_df, test_df)

def test_directory_operations(
    s3_client: S3,
    test_dir: str,
    test_files: list[str]
) -> None:
    """Test directory listing and tree structure."""
    # Create test files
    files = [
        f"file1_{uuid.uuid4().hex[:8]}.json",
        f"file2_{uuid.uuid4().hex[:8]}.parquet",
        f"subfolder/file3_{uuid.uuid4().hex[:8]}.json"
    ]
    
    # Upload test files
    for file_path in files:
        full_path = f"{test_dir}{file_path}"
        if file_path.endswith('.json'):
            s3_client.upload_object({"test": "data"}, full_path)
        else:
            s3_client.upload_object(pd.DataFrame({'test': [1, 2, 3]}), full_path)
        test_files.append(file_path)

    # Get directory tree
    tree = s3_client.get_directory_tree(test_dir)
    
    # Verify structure
    assert 'files' in tree
    assert 'subfolder' in tree
    assert len(tree['files']) == 2  # Two files in root
    assert 'files' in tree['subfolder']  # One file in subfolder
    assert len(tree['subfolder']['files']) == 1

def test_copy_operations(
    s3_client: S3,
    test_dir: str,
    test_files: list[str]
) -> None:
    """Test file copy operations."""
    # Create source file
    source_file = f"source_{uuid.uuid4().hex[:8]}.json"
    dest_file = f"dest_{uuid.uuid4().hex[:8]}.json"
    test_files.extend([source_file, dest_file])
    
    source_path = f"{test_dir}{source_file}"
    dest_path = f"{test_dir}{dest_file}"
    
    # Upload source file
    test_data = {"test": "copy_data"}
    s3_client.upload_object(test_data, source_path)
    
    # Copy file
    s3_client.copy_object(source_path, dest_path)
    
    # Verify copy
    copied_data = s3_client.get_object(dest_path)
    assert copied_data == test_data

def test_combine_parquet_files(
    s3_client: S3,
    test_dir: str,
    test_files: list[str]
) -> None:
    """Test combining multiple Parquet files."""
    # Create test files
    dfs = [
        pd.DataFrame({
            'id': range(i*100, (i+1)*100),
            'value': [f"batch_{i}"] * 100
        })
        for i in range(3)
    ]
    
    files = []
    for i, df in enumerate(dfs):
        file_path = f"part_{i}_{uuid.uuid4().hex[:8]}.parquet"
        full_path = f"{test_dir}{file_path}"
        s3_client.upload_object(df, full_path)
        files.append(full_path)
        test_files.append(file_path)

    # Combined file
    combined_file = f"combined_{uuid.uuid4().hex[:8]}.parquet"
    combined_path = f"{test_dir}{combined_file}"
    test_files.append(combined_file)

    # Combine files
    s3_client.combine_parquet_files(combined_path, files)

    # Verify combined file
    combined_df = s3_client.get_object(combined_path)
    assert len(combined_df) == 300  # Total rows from all files
    assert all(col in combined_df.columns for col in ['id', 'value'])

def test_error_scenarios(
    s3_client: S3,
    test_dir: str,
    test_files: list[str]
) -> None:
    """Test error handling scenarios."""
    # Test invalid file type
    with pytest.raises(Exception) as exc_info:
        s3_client.upload_object({"test": "data"}, f"{test_dir}test.txt")
    assert "Unsupported file type" in str(exc_info.value)

    # Test non-existent file
    with pytest.raises(Exception) as exc_info:
        s3_client.get_object(f"{test_dir}nonexistent.json")
    assert "Failed to get" in str(exc_info.value)

    # Test invalid JSON data
    invalid_json = object()  # Cannot be serialized to JSON
    with pytest.raises(Exception) as exc_info:
        s3_client.upload_object(invalid_json, f"{test_dir}test.json")
    assert "Failed to upload" in str(exc_info.value)

@pytest.fixture(autouse=True)
def cleanup(
    s3_client: S3,
    test_dir: str,
    test_files: list[str]
) -> Generator[None, None, None]:
    """Clean up test files after each test."""
    yield
    for file_path in test_files:
        try:
            s3_client.delete(f"{test_dir}{file_path}")
        except Exception:
            pass  # Ignore errors during cleanup
