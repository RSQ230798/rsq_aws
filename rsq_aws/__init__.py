"""
rsq_aws - A comprehensive Python library for AWS services
=======================================================

This library provides a simplified interface for working with AWS services,
particularly S3 and Redshift.

Main Components:
---------------
- S3: Interface for AWS S3 operations (upload, download, management)
- PathFactory: Utility for handling S3 paths with parameter substitution
- BotoConnection: Interface for AWS Redshift operations using redshift-data API
update 2
"""

from rsq_aws.s3.s3 import S3
from rsq_aws.s3.path_factory import PathFactory
from rsq_aws.redshift.boto_connection import BotoConnection
from rsq_aws.redshift.psycopg_connection import PsycopgConnection

__version__ = "1.0.0"

__all__ = [
    "S3",
    "PathFactory",
    "BotoConnection",
    "PsycopgConnection"
]