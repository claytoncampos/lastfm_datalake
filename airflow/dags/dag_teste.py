import urllib3
import pathlib
import json

from minio import Minio
from pendulum import datetime
from airflow.decorators import (
    dag,
    task,
)  # DAG and task decorators for interfacing with the TaskFlow API

CWD = pathlib.Path(__file__).parent
default_args = {
    "owner": "Clayton de Almeida Campos",
    "retries": 1,
    "retry_delay": 0
}

# When using the DAG decorator, The "dag_id" value defaults to the name of the function
# it is decorating if not explicitly set. In this example, the "dag_id" value would be "example_dag_basic".
@dag(
    # This defines how often your DAG will run, or the schedule by which your DAG runs. In this case, this DAG
    # will run daily
    schedule="@daily",
    # This DAG is set to run for the first time on January 1, 2023. Best practice is to use a static
    # start_date. Subsequent DAG runs are instantiated based on the schedule
    start_date=datetime(2024, 10, 30),
    # When catchup=False, your DAG will only run the latest run that would have been scheduled. In this case, this means
    # that tasks will not be run between January 1, 2023 and 30 mins ago. When turned on, this DAG's first
    # run will be for the next 30 mins, per the its schedule
    catchup=False,
    default_args=default_args,
    tags=["upload_files_bucket"],
)  # If set, this tag is shown in the DAG view of the Airflow UI
def upload():
    @task
    def up_files():
        # Since we are using self-signed certs we need to disable TLS verification
        http_client = urllib3.PoolManager(cert_reqs='CERT_NONE')
        urllib3.disable_warnings()

        # Initialize MinIO client
        minio_client = Minio(endpoint="host.docker.internal:9000",
                             secure=False,
                             access_key="minioadmin",
                             secret_key="minioadmin123",
                             http_client=http_client
                             )

        # Create destination bucket if it does not exist
        if not minio_client.bucket_exists("processed"):
          minio_client.make_bucket("processed")
          print("teste")
        # df = pd.read_json(str(CWD.parent) + "/dags/data/user/user*"),
        # df = pd.read_csv("https://storage.centerville.oak-tree.tech/public/examples/test.csv")

        # csv = df.to_csv().encode('utf-8')

        # minio_client.fput_object(
        #     bucket_name="processed",
        #     object_name="agora.json",
        #     file_path=(str(CWD.parent) + "/dags/data/user/user_2023_2_28_23_30_28.json")
        # )

        # (minio_client.fput_object("processed",
        #                         "user_2023_2_28_23_30_28.json",
        #                         "/dags/data/user/user_2023_2_28_23_30_28.json",
        #                          ))

        print("- Uploaded processed object  to Destination Bucket ")
        up_files()


upload()