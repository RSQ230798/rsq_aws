from typing import Dict, List, Optional, Any, Union
import boto3
import pandas as pd

from src.rsq_aws.redshift._private._interfaces import RedshiftConnection

class BotoConnection(RedshiftConnection):
    """
    A class to handle AWS Redshift data operations using the redshift-data API.
    
    This class provides an interface for executing SQL queries against Redshift
    and retrieving results as pandas DataFrames.
    """

    def __init__(self, workgroup: str, database: str, region: str) -> None:
        """
        Initialize Redshift client with AWS credentials.

        Args:
            workgroup: Name of the Redshift workgroup
            database: Name of the database to connect to
            region: AWS region name

        Raises:
            Exception: If connection to Redshift fails
        """
        super().__init__(workgroup, database, region)
        self.client = boto3.client('redshift-data', region=self.region)

    def query(self, sql: str) -> pd.DataFrame:
        """
        Execute a SQL query and return results as a pandas DataFrame.

        Args:
            sql: SQL query to execute

        Returns:
            DataFrame containing query results

        Raises:
            Exception: If query execution fails
        """
        response = self._execute_query(sql)
        query_id = self._get_query_id(response)
        return self._get_query_results(query_id)

    def _execute_query(self, sql_query: str) -> Dict[str, Any]:
        """Execute a SQL query and return the response."""
        return self.client.execute_statement(
            WorkgroupName=self.workgroup,
            Database=self.database,
            Sql=sql_query
        )

    def _get_query_id(self, response: Dict[str, Any]) -> str:
        """Extract query ID from response."""
        return response['Id']
    
    def _get_query_results(self, query_id: str) -> pd.DataFrame:
        """Get query results for a given query ID."""
        self._wait_for_query_to_run(query_id)
        self._check_query_status(query_id)
        return self._get_results(query_id)
        
    def _wait_for_query_to_run(self, query_id: str) -> None:
        """Wait for query execution to complete."""
        while True:
            status = self.client.describe_statement(Id=query_id)
            if status['Status'] in ['FINISHED', 'FAILED']:
                break

    def _check_query_status(self, query_id: str) -> None:
        """Check if query completed successfully."""
        status = self.client.describe_statement(Id=query_id)
        if status['Status'] == 'FAILED':
            raise Exception(f"Query failed: {status['Error']}")
    
    def _get_results(self, query_id: str) -> pd.DataFrame:
        """Retrieve and format query results."""
        if self._has_result_set(query_id):
            results = self.client.get_statement_result(Id=query_id)
            return self._format_query_results_as_df(results)
        return pd.DataFrame()

    def _has_result_set(self, query_id: str) -> bool:
        """Check if query has results."""
        result_description = self.client.describe_statement(Id=query_id)
        return result_description["HasResultSet"]

    def _format_query_results_as_df(self, query_results: Dict[str, Any]) -> pd.DataFrame:
        """Format query results as a pandas DataFrame."""
        if query_results is None:
            return pd.DataFrame()
        
        json_results = self._format_query_results_as_json(query_results)
        return pd.DataFrame(json_results)

    def _format_query_results_as_json(self, query_results: Dict[str, Any]) -> Dict[str, List[Any]]:
        """Convert query results to JSON format."""
        columns = self._get_columns_from_query_results(query_results)
        data = {col: [] for col in columns}
        
        for row in query_results["Records"]:
            for i, col in enumerate(columns):
                data[col].append(list(row[i].values())[0])
        
        return data

    def _get_columns_from_query_results(self, query_results: Dict[str, Any]) -> List[str]:
        """Extract column names from query results."""
        return [v["name"] for v in query_results["ColumnMetadata"]]
