# RSQ AWS Library

A comprehensive Python library that provides a simplified interface for AWS services, specifically designed for interacting with Amazon S3 and Redshift. This library streamlines common AWS operations and adds enhanced functionality for data management and database operations.

## Features

### S3 Module
- **File Management**
  - Upload/download files (JSON, Parquet)
  - Directory traversal and listing
  - Object operations (copy, delete)
- **Data Handling**
  - Direct DataFrame to Parquet conversion
  - JSON object management
  - Batch processing for large files
  - Parquet file combination utilities

### Redshift Module
- **Connection Management**
  - Boto3-based connections
  - Psycopg2 direct database access
- **Query Execution**
  - SQL query execution with DataFrame results
  - Secure credential management
  - Workgroup-based authentication

## Installation

```bash
pip install -r requirements.txt
```

## Dependencies

- boto3 (1.36.21)
- psycopg2 (2.9.10)
- pandas (2.2.3)
- pyarrow (19.0.0)
- python-dotenv (1.0.1)
- Additional dependencies listed in requirements.txt

## Usage Examples

### S3 Operations

```python
from rsq_aws.s3.core.s3 import S3

# Initialize S3 client
s3_client = S3(
    bucket="your-bucket",
    access_key_id="your-access-key",
    secret_access_key="your-secret-key",
    region_name="your-region"
)

# Upload DataFrame as Parquet
s3_client.upload_object(df, "path/to/file.parquet")

# Download JSON file
data = s3_client.get_object("path/to/file.json")

# List directory contents
tree = s3_client.get_directory_tree("path/to/directory")
```

### Redshift Operations

```python
from rsq_aws.redshift.core.psycopg_connection import PsycopgConnection

# Initialize Redshift connection
redshift = PsycopgConnection(
    database="your-database",
    workgroup="your-workgroup",
    host="your-host",
    port=5439,
    region="your-region"
)

# Execute query
df = redshift.query("SELECT * FROM your_table")

# Close connection
redshift.close()
```

## Module Documentation

### S3 Core
- `path_factory.py`: Base class for generating S3 file paths
- `s3.py`: Main S3 connection handler with enhanced functionality

### Redshift Core
- `boto_connection.py`: AWS Boto3-based Redshift connection
- `psycopg_connection.py`: Direct Psycopg2 database connection
- `credentials.py`: Secure credential management

## Contributing

1. Fork the repository
2. Create a feature branch
3. Commit your changes
4. Push to the branch
5. Create a Pull Request

## License

This project is proprietary and all rights are reserved.
