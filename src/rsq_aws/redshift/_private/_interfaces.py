from typing import Protocol
import pandas as pd

class RedshiftConnection(Protocol):
    """Protocol defining interface for Redshift database connections."""
    workgroup: str
    database: str
    region: str

    def query(self, sql: str) -> pd.DataFrame:
        """
        Execute a SQL query and return results as a pandas DataFrame.

        Args:
            sql: SQL query to execute 
        
        Returns:
            DataFrame containing query results
        
        Example:
            >>> connection = RedshiftConnectionImpl(workgroup="dev", database="analytics", region="us-east-1")
            >>> df = connection.query("SELECT * FROM users LIMIT 10")
            >>> print(df.head())
        """
        ...
