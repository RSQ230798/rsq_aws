from typing import Dict, List, Optional, Union, Any
import pandas as pd
import json
import boto3
import io
import pyarrow.parquet as pq
import pyarrow as pa

from rsq_utils.paths import clean_path

class S3:
    """
    A class to handle AWS S3 operations including file upload, download, and management.
    
    This class provides a simplified interface for common S3 operations including:
    - Directory traversal and listing
    - File upload and download
    - Object manipulation (copy, delete)
    - Special handling for JSON and Parquet files
    """

    def __init__(self, bucket: str, access_key_id: str, secret_access_key: str, region_name: str):
        """
        Initialize S3 client with AWS credentials.

        Args:
            bucket: Name of the S3 bucket
            access_key_id: AWS access key ID
            secret_access_key: AWS secret access key
            region_name: AWS region name

        Raises:
            Exception: If connection to S3 fails
        """
        self.bucket = bucket
        self.access_key_id = access_key_id
        self.secret_access_key = secret_access_key
        self.region_name = region_name

        try:
            self.s3 = boto3.client(
                's3',
                aws_access_key_id=self.access_key_id,
                aws_secret_access_key=self.secret_access_key,
                region_name=self.region_name
            )
        except Exception as e:
            raise Exception(f"Failed to connect to S3: {str(e)}")

    def get_directory_tree(self, path: str) -> Dict[str, Any]:
        """
        Recursively get the directory tree structure starting from the given path.

        Args:
            path: The S3 path to start from

        Returns:
            Dict containing the directory tree structure with files and folders
        """
        path = clean_path(path)
        directory_data = self.get_directory(path)
        files = self._extract_files_from_directory_data(directory_data)
        folders = self._extract_folders_from_directory_data(directory_data)

        tree: Dict[str, Any] = {}
        if files:
            tree["files"] = files

        if folders:
            for folder in folders:
                new_path = self._create_new_path(path, folder)
                tree[folder] = self.get_directory_tree(new_path)

        return tree

    def get_directory(self, path: str) -> List[str]:
        """
        Get contents of a directory at the specified path.

        Args:
            path: The S3 path to list contents from

        Returns:
            List of file and folder names in the directory

        Raises:
            Exception: If no files are found in the directory
        """
        path = clean_path(path)
        files = self._fetch_files_from_s3_path(path)

        if not files:
            raise Exception(f"No files found in directory: {path}")

        files = list(set([file.replace(path, "").split("/")[0] for file in files]))
        return [file for file in files if file]

    def upload_object(self, data: Union[Dict, pd.DataFrame], path: str) -> str:
        """
        Upload data as either JSON or Parquet file to S3.

        Args:
            data: Dictionary for JSON or DataFrame for Parquet
            path: Destination path in S3

        Returns:
            Success message

        Raises:
            Exception: If file type is not supported or upload fails
        """
        path = clean_path(path)

        if path.endswith(".json") and isinstance(data, dict):
            return self._upload_json_object(data, path)
        elif path.endswith(".parquet") and isinstance(data, pd.DataFrame):
            return self._upload_parquet_object(data, path)
        else:
            raise Exception("Unsupported file type. Only .json and .parquet files are supported.")

    def upload_file(self, local_path: str, path: str) -> str:
        """
        Upload a local file to S3.

        Args:
            local_path: Path to local file
            path: Destination path in S3

        Returns:
            Success message

        Raises:
            Exception: If upload fails
        """
        path = clean_path(path)
        try:
            self.s3.upload_file(local_path, self.bucket, path)
            return "File uploaded successfully!"
        except Exception as e:
            raise Exception(f"Failed to upload file to S3: {str(e)}")

    def get_object(self, path: str) -> Union[Dict, pd.DataFrame]:
        """
        Retrieve an object from S3 as either JSON or Parquet.

        Args:
            path: Path to the object in S3

        Returns:
            Dictionary for JSON files or DataFrame for Parquet files

        Raises:
            Exception: If file type is not supported or retrieval fails
        """
        path = clean_path(path)
        if path.endswith(".json"):
            output_json: Dict = self._get_json_object(path)
            return output_json
        elif path.endswith(".parquet"):
            output_df: pd.DataFrame = self._get_parquet_object_via_pandas(path)
            return output_df
        else:
            raise Exception("Unsupported file type. Only .json and .parquet files are supported.")

    def download_file(self, path: str, local_path: str) -> str:
        """
        Download a file from S3 to local storage.

        Args:
            path: Path to the file in S3
            local_path: Local destination path

        Returns:
            Success message

        Raises:
            Exception: If download fails
        """
        path = clean_path(path)
        try:
            self.s3.download_file(self.bucket, path, local_path)
            return "File downloaded successfully!"
        except Exception as e:
            raise Exception(f"Failed to download file from S3: {str(e)}")

    def delete(self, path: str) -> str:
        """
        Delete an object from S3.

        Args:
            path: Path to the object in S3

        Returns:
            Success message

        Raises:
            Exception: If deletion fails
        """
        path = clean_path(path)
        try:
            self.s3.delete_object(Bucket=self.bucket, Key=path)
            return "File deleted successfully!"
        except Exception as e:
            raise Exception(f"Failed to delete file from S3: {str(e)}")

    def copy_object(self, source_path: str, destination_path: str, must_delete: bool = False) -> None:
        """
        Copy an object within the same bucket, with option to delete the source.

        Args:
            source_path: Path to the source object
            destination_path: Destination path
            must_delete: Whether to delete the source object after copying

        Raises:
            Exception: If copy operation fails
        """
        try:
            self.s3.copy_object(
                CopySource={'Bucket': self.bucket, 'Key': source_path},
                Bucket=self.bucket,
                Key=destination_path
            )
            print(f"Object copied successfully to {destination_path}")

            if must_delete:
                self.delete(source_path)
        except Exception as e:
            raise Exception(f"Failed to copy object: {str(e)}")

    def combine_parquet_files(self, output_path: str, files: List[str], batch_size: int = 10000) -> None:
        """
        Combine multiple Parquet files into a single file.

        Args:
            output_path: Path for the combined output file
            files: List of Parquet files to combine
            batch_size: Number of rows to process at once

        Raises:
            Exception: If no Parquet files found or combination fails
        """
        files = [file for file in files if file.endswith(".parquet")]

        if not files:
            raise Exception("No Parquet files found in the provided list")

        combined_buffer = self._combine_parquet_files_into_output_buffer(files, batch_size)
        self._upload_output_buffer(combined_buffer, output_path)
        print(f"Combined Parquet files uploaded to {output_path}")

    def _fetch_files_from_s3_path(self, path: str) -> List[str]:
        """Fetch all files from an S3 path, handling pagination."""
        files: List[str] = []
        paginator = self.s3.get_paginator('list_objects_v2')
        
        for page in paginator.paginate(Bucket=self.bucket, Prefix=path):
            if 'Contents' in page:
                files.extend(obj['Key'] for obj in page['Contents'])
        
        return files

    def _extract_files_from_directory_data(self, directory_data: List[str]) -> Optional[List[str]]:
        """Extract file names from directory data."""
        files = [file for file in directory_data if "." in file]
        return files if files else None

    def _extract_folders_from_directory_data(self, directory_data: List[str]) -> Optional[List[str]]:
        """Extract folder names from directory data."""
        folders = [folder for folder in directory_data if "." not in folder]
        return folders if folders else None

    def _create_new_path(self, old_path: str, suffix: str) -> str:
        """Create a new path by combining old path and suffix."""
        return str(clean_path(old_path) + clean_path(suffix))

    def _upload_json_object(self, data: Dict, path: str) -> str:
        """Upload a JSON object to S3."""
        try:
            json_data = json.dumps(data)
            self.s3.put_object(
                Bucket=self.bucket,
                Key=path,
                Body=json_data,
                ContentType='application/json'
            )
            return "JSON file uploaded successfully!"
        except Exception as e:
            raise Exception(f"Failed to upload JSON file to S3: {str(e)}")

    def _upload_parquet_object(self, data: pd.DataFrame, path: str) -> str:
        """Upload a Parquet object to S3."""
        try:
            with io.BytesIO() as parquet_buffer:
                data.to_parquet(parquet_buffer, index=True, engine="pyarrow")
                parquet_buffer.seek(0)
                self.s3.put_object(
                    Bucket=self.bucket,
                    Key=path,
                    Body=parquet_buffer.getvalue(),
                    ContentType='application/octet-stream'
                )
            return "Parquet file uploaded successfully!"
        except Exception as e:
            raise Exception(f"Failed to upload Parquet file to S3: {str(e)}")

    def _upload_output_buffer(self, output_buffer: io.BytesIO, path: str) -> str:
        """Upload a BytesIO buffer to S3."""
        try:
            self.s3.put_object(Bucket=self.bucket, Key=path, Body=output_buffer)
            return "File uploaded successfully!"
        except Exception as e:
            raise Exception(f"Failed to upload file to S3: {str(e)}")

    def _get_json_object(self, path: str) -> Dict:
        """Retrieve a JSON object from S3."""
        try:
            obj = self.s3.get_object(Bucket=self.bucket, Key=path)
            output: Dict = json.loads(obj['Body'].read().decode('utf-8'))
            return output
        except Exception as e:
            raise Exception(f"Failed to get JSON file: {str(e)}")

    def _get_parquet_object_via_pandas(self, path: str) -> pd.DataFrame:
        """Retrieve a Parquet object from S3 using pandas."""
        try:
            obj = self.s3.get_object(Bucket=self.bucket, Key=path)
            return pd.read_parquet(io.BytesIO(obj['Body'].read()), engine="pyarrow")
        except Exception as e:
            raise Exception(f"Failed to get Parquet file: {str(e)}")

    def _get_parquet_object_via_pyarrow(self, path: str) -> pq.ParquetFile:
        """Retrieve a Parquet object from S3 using pyarrow."""
        try:
            obj = self.s3.get_object(Bucket=self.bucket, Key=path)
            return pq.ParquetFile(io.BytesIO(obj['Body'].read()))
        except Exception as e:
            raise Exception(f"Failed to get Parquet file: {str(e)}")

    def _combine_parquet_files_into_output_buffer(self, files: List[str], batch_size: int = 10000) -> io.BytesIO:
        """Combine multiple Parquet files into a single BytesIO buffer."""
        writer = None
        output_buffer = io.BytesIO()

        for file in files:
            print(f"Reading file: {file}")
            parquet_file = self._get_parquet_object_via_pyarrow(file)

            for batch in parquet_file.iter_batches(batch_size=batch_size):
                df = pa.Table.from_batches([batch]).to_pandas()

                if writer is None:
                    writer = pq.ParquetWriter(output_buffer, pa.Table.from_pandas(df).schema)

                writer.write_table(pa.Table.from_pandas(df))

        if writer is not None:
            writer.close()
            output_buffer.seek(0)
            return output_buffer
        else:
            raise Exception("No data was written. Please check the input files.")
