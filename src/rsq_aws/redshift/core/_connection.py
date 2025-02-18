from abc import ABC, abstractmethod
import pandas as pd

class RedshiftConnection(ABC):
    def __init__(self, workgroup: str, database: str, region: str):
        self.workgroup = workgroup
        self.database = database
        self.region = region

    @abstractmethod
    def query(self, sql: str) -> pd.DataFrame:
        """
        Execute a SQL query and return results as a pandas DataFrame.

        Args:
            sql: SQL query to execute 
        
        Returns:
            DataFrame containing query results
        """


