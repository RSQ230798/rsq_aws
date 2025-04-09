import boto3
from typing import Dict

class Credentials:
    def __init__(self, workgroup: str, region: str) -> None:
        self.region: str = region
        self.workgroup: str = workgroup
        self.client = boto3.Session(region_name=self.region).client("redshift-serverless")
        
    def get(self) -> Dict[str, str]:
        results: Dict[str, str] = self.client.get_credentials(workgroupName=self.workgroup)
        return results