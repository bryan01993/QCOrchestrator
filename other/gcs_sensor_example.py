from datetime import datetime
import os
import google
import pendulum
from airflow import DAG
from airflow.decorators import task
from airflow.operators.empty import EmptyOperator
from airflow.providers.google.cloud.sensors.gcs import GCSObjectsWithPrefixExistenceSensor

doc_md = """
### lakehouse-ingestion-pipeline
#### Purpose
Load all the raw files from the defined sources (host, Flash, external sources...)
"""

credentials, project_id = google.auth.load_credentials_from_file(
    "/opt/airflow/dags/afb-lakehouse-poc-f00756f9a1a7.json")
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = "/opt/airflow/dags/afb-lakehouse-poc-f00756f9a1a7.json"

with DAG(
        dag_id='afb-sensor-example',
        doc_md=doc_md,
        start_date=pendulum.datetime(2023, 2, 20, tz="UTC"),
        # end_date=pendulum.datetime(2023, 2, 23, tz="UTC"),
        schedule=None,
        catchup=False,
        default_args={'retries': 3},
        tags=["example"],
) as dag:
    @task.python(task_id="currentdate")
    def get_date(**context) -> str:
        # return str(datetime.now().strftime('%y%m%d'))
        ti = context["ti"]
        date = context["execution_date"].strftime('%y%m%d')
        ti.xcom_push(key='descomprimir', value=date)


    # start = EmptyOperator(task_id='start')

    reach_file_in_gcs = GCSObjectsWithPrefixExistenceSensor(task_id='REACH_FILE_GCS',
                                                            bucket='afb-input-reception',
                                                            prefix="{{ti.xcom_pull(task_ids='currentdate', key='descomprimir')}}/prueba_{{ti.xcom_pull(task_ids='currentdate', key='descomprimir')}}.csv",
                                                            google_cloud_conn_id='google_cloud_default')


    @task.python(task_id="sensor")
    def print_file(**context):
        filepath = context['ti'].xcom_pull(task_ids="REACH_FILE_GCS", key='return_value')
        print(filepath)


    printfile_op = print_file()
    currentdate_op = get_date()

    currentdate_op >> reach_file_in_gcs >> printfile_op
