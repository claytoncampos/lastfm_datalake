from datetime import timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from airflow.utils.task_group import TaskGroup

from util.lastfm import LastfmAPI
from util.storage import Storage

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

with DAG(
    'dag_lastfm',
    default_args=default_args,
    description='lastfm data lake',
    schedule_interval=timedelta(days=1),
    start_date=days_ago(1),
    tags=['lastfm', 'landing'],
) as dag:
    with TaskGroup(group_id='lastfm_ingestion') as lastfm_ingestion:
        def ingestion(country: str, s3_path: str):
            api = LastfmAPI()
            json_data = api.get_top_songs_by_country(country=country)
            storage = Storage()
            storage.save_top_songs_by_country_to_bucket(data=json_data, path=s3_path)

        PythonOperator(
            task_id='lastfm_brazil_ingestion',
            python_callable=ingestion,
            op_kwargs={'country': 'brazil', 's3_path': 's3a://landing/lastfm_recommend_tracks_country_brazil/'},
        )
        PythonOperator(
            task_id='lastfm_argentina_ingestion',
            python_callable=ingestion,
            op_kwargs={'country': 'argentina', 's3_path': 's3a://landing/lastfm_recommend_tracks_country_argentina/'},
        )
        PythonOperator(
            task_id='lastfm_spain_ingestion',
            python_callable=ingestion,
            op_kwargs={'country': 'spain', 's3_path': 's3a://landing/lastfm_recommend_tracks_country_spain/'},
        )
        PythonOperator(
            task_id='lastfm_portugal_ingestion',
            python_callable=ingestion,
            op_kwargs={'country': 'portugal', 's3_path': 's3a://landing/lastfm_recommend_tracks_country_portugal/'},
        )

    # with TaskGroup(group_id='lastfm_transformation') as lastfm_transformation:
    #     dbt_build = BashOperator(
    #         task_id='dbt_build',
    #         bash_command='cd /opt/airflow/dags/dbt_project && dbt deps && dbt build --profiles-dir .',
    #     )

    lastfm_ingestion 
