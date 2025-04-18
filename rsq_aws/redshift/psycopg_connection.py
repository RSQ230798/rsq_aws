from typing import Dict, List, Optional, Any, Union
import psycopg2
import pandas as pd

from rsq_aws.redshift._private._interfaces import RedshiftConnection
from rsq_aws.redshift._private._helpers import Credentials

class PsycopgConnection:
    """
    A class to handle Redshift connections using psycopg2.
    
    This class provides an interface for executing SQL queries against Redshift
    using a direct psycopg2 connection and retrieving results as pandas DataFrames.
    """

    def __init__(self, workgroup: str, database: str, region: str, host: str, port: int) -> None:
        """
        Initialize Redshift connection using psycopg2.

        Args:
            database: Name of the database to connect to
            workgroup: Name of the Redshift workgroup
            host: Hostname of the Redshift cluster
            port: Port number for the connection

        Raises:
            Exception: If connection to Redshift fails
        """
        self.workgroup: str = workgroup
        self.database: str = database
        self.region: str = region
        self.host: str = host
        self.port: int = port
        self.connection: psycopg2.extensions.connection = None

    def query(self, sql: str) -> pd.DataFrame:
        """
        Execute a SQL query and return results as a pandas DataFrame.

        Args:
            sql: SQL query to execute

        Returns:
            DataFrame containing query results

        Raises:
            Exception: If query execution fails or connection is lost
        """
        if not self._is_connected():
            self._connect()
        
        cursor = self.connection.cursor()
        cursor.execute(sql)

        data = cursor.fetchall()
        description = cursor.description

        return self._generate_df(data, description)        

    def close(self) -> None:
        """Close the database connection."""
        if self.connection is not None:
            self.connection.close()
            self.connection = None

    def _is_connected(self) -> bool:
        """Check if database connection is active."""
        return self.connection is not None

    def _connect(self) -> None:
        """Establish database connection using credentials."""
        try:
            credentials = self._get_credentials()
            self.connection = psycopg2.connect(
                dbname=self.database,
                user=credentials.get("dbUser", "test-user"),
                password=credentials.get("dbPassword", "test-password"),
                host=self.host,
                port=self.port
            )
        except Exception as e:
            self.connection = None
            raise e

    def _get_credentials(self) -> Dict[str, str]:
        """Retrieve database credentials."""
        return Credentials(self.workgroup, self.region).get()

    def _generate_df(self, data: List[tuple], description: List[tuple]) -> pd.DataFrame:
        """Generate DataFrame from query results."""
        columns = [desc[0] for desc in description]    
        return pd.DataFrame(data, columns=columns)
