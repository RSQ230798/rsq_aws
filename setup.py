from setuptools import setup, find_packages

setup(
    name="aws",
    version="0.1.0",
    packages=find_packages(where="src"),
    package_dir={"": "src"},
    install_requires=[
        "boto3==1.36.21",
        "botocore==1.36.21",
        "jmespath==1.0.1",
        "numpy==2.2.3",
        "pandas==2.2.3",
        "psycopg2==2.9.10",
        "pyarrow==19.0.0",
        "python-dateutil==2.9.0.post0",
        "python-dotenv==1.0.1",
        "pytz==2025.1",
        "s3transfer==0.11.2",
        "six==1.17.0",
        "tzdata==2025.1",
        "urllib3==2.3.0",
        "rsq_utils @ git+https://github.com/rsq230798/rsq_utils.git"
    ],
    python_requires=">=3.8",
)
