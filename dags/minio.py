from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import boto3

# Configuration for MinIO
MINIO_ENDPOINT = "http://minio.default.svc.cluster.local:9000"  # Replace with your MinIO endpoint
MINIO_ACCESS_KEY = "KgyMpLt8zudSxsek90wM"  # Replace with your access key
MINIO_SECRET_KEY = "VaP6K153yq7r4lAGU0LdEIBNRwFSFGiENhy0AIDd"  # Replace with your secret key
MINIO_BUCKET = "example-bucket"  # Replace with your bucket name

# Function to connect to MinIO and list buckets
def connect_to_minio(**kwargs):
    # Initialize the S3 client for MinIO
    s3_client = boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
    )
    
    # Create a bucket if it doesn't exist
    try:
        s3_client.create_bucket(Bucket=MINIO_BUCKET)
        print(f"Bucket '{MINIO_BUCKET}' created successfully.")
    except s3_client.exceptions.BucketAlreadyOwnedByYou:
        print(f"Bucket '{MINIO_BUCKET}' already exists.")
    
    # List all buckets
    response = s3_client.list_buckets()
    print("Buckets:")
    for bucket in response["Buckets"]:
        print(f"  - {bucket['Name']}")

# Define the Airflow DAG
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
}
dag = DAG(
    dag_id="minio_connect_dag",
    default_args=default_args,
    description="A DAG to connect to MinIO and list buckets",
    schedule_interval=None,  # Run on demand
    start_date=datetime(2023, 1, 1),
    catchup=False,
)

# Task to connect to MinIO
minio_task = PythonOperator(
    task_id="connect_to_minio",
    python_callable=connect_to_minio,
    provide_context=True,
    dag=dag,
)

# Set task dependencies
minio_task
