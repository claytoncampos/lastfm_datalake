# import libraries
import os
import pathlib
import urllib3


from datetime import datetime, timedelta
from minio import Minio

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator, task

# connections
S3_CONN_ID = "aws_default"
OUTPUT_CONN_ID = "aws_default"

print(os.environ.get('SPOTIFY_CLIENT_ID'))
print(os.environ.get('MINIO_ENDPOINT'))
# default args & init dag
CWD = pathlib.Path(__file__)

default_args = {
    "owner": "Clayton de Almeida Campos",
    "retries": 1,
    "retry_delay": 0
}

my_dag = DAG(dag_id="create_bucket_minio",
             start_date=datetime(2024, 1, 23),
             max_active_runs=1,
             schedule_interval=timedelta(hours=24),
             default_args=default_args,
             catchup=False,
             tags=['development', 'elt', 'astrosdk', 'minio', 'bucket']
             )

# init & finish task
init_data_load = EmptyOperator(task_id="init")
finish_data_load = EmptyOperator(task_id="finish")


def create_buckets():
    # Since we are using self-signed certs we need to disable TLS verification
    http_client = urllib3.PoolManager(cert_reqs='CERT_NONE')
    urllib3.disable_warnings()

    # Initialize MinIO client
    minio_client = Minio(endpoint=os.environ.get('MINIO_ENDPOINT'),
                         secure=False,
                         access_key=os.environ.get('MINIO_ACCESS_KEY'),
                         secret_key=os.environ.get('MINIO_SECRET_KEY'),
                         http_client=http_client
                         )

    # Create destination bucket if it does not exist
    buckets=["bronze","silver","gold","bianca2"]
    for i in buckets:
        if not minio_client.bucket_exists(i):
            minio_client.make_bucket(i)
            print(f"created bucket {i} in minio ")


def upload_files():
    # Since we are using self-signed certs we need to disable TLS verification
    http_client = urllib3.PoolManager(cert_reqs='CERT_NONE')
    urllib3.disable_warnings()


    # Initialize MinIO client
    minio_client = Minio(endpoint=os.environ.get('MINIO_ENDPOINT'),
                         secure=False,
                         access_key=os.environ.get('MINIO_ACCESS_KEY'),
                         secret_key=os.environ.get('MINIO_SECRET_KEY'),
                         http_client=http_client
                         )
    # upload file local to s3 minio
    minio_client.fput_object(
            bucket_name="bronze",
            object_name="teste.csv",
            file_path=(str(CWD.parent) + "/teste.csv")
        )
    
    
make_buckets = PythonOperator(
    task_id='create_buckets_minio',
    python_callable=create_buckets,
    provide_context=True,
    dag=my_dag
)

uplod_files_s3 = PythonOperator(
    task_id='upload_files',
    python_callable=upload_files,
    provide_context=True,
    dag=my_dag
)



 # define sequence
init_data_load >> make_buckets >> uplod_files_s3 >> finish_data_load
# init_data_load >> make_buckets >> finish_data_load
