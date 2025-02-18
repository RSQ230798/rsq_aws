from abc import ABC, abstractmethod
import pandas as pd

class RedshiftConnection(ABC):
    """Protocol defining interface for Redshift database connections."""
    def __init__(self, workgroup: str, database: str, region: str) -> None:  
        self.workgroup: str = workgroup
        self.database: str = database
        self.region: str = region

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
        pass
