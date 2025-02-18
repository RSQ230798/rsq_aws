import boto3

class Credentials:
    def __init__(self, workgroup, region):
        self.region = region
        self.workgroup = workgroup
        self.client = boto3.Session(region=self.region).client("redshift-serverless")
        
    def get(self):
        return self.client.get_credentials(workgroupName=self.workgroup)
