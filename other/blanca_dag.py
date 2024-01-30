import logging
import os
import pendulum
import re
from datetime import datetime

import google.auth
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.gcs import GCSListObjectsOperator
from airflow.providers.http.operators.http import SimpleHttpOperator
from google.cloud import bigquery
from google.cloud import logging as gcp_logging
from google.cloud.logging_v2.handlers import CloudLoggingHandler

# from airflow.models import Variable
# from airflow.operators.empty import EmptyOperator
from airflow.providers.google.cloud.sensors.gcs import GCSObjectsWithPrefixExistenceSensor

# from astronomer.providers.google.cloud.sensors.gcs import GCSObjectsWithPrefixExistenceSensorAsync
# from airflow.providers.google.cloud.transfers.gcs_to_gcs import GCSToGCSOperator
# from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
# from airflow.providers.google.cloud.operators.dataproc import DataprocCreateBatchOperator

credentials, project_id = google.auth.load_credentials_from_file(
    "/opt/airflow/dags/afb-lakehouse-poc-f00756f9a1a7.json")
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = "/opt/airflow/dags/afb-lakehouse-poc-f00756f9a1a7.json"

client = gcp_logging.Client(project='afb-lakehouse-poc')
handler = CloudLoggingHandler(client)
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
logger.addHandler(handler)

bq_client = bigquery.Client('afb-lakehouse-poc')

query = """
        SELECT DISTINCT TABLE_NAME, RAW_FILE_NAME FROM afb-lakehouse-poc.control.dl_conf_batch_exec
        WHERE IND_ACTIVE = true
"""
df = bq_client.query(query).to_dataframe()
# tables_list = list(df['TABLE_NAME'])
# tables_list = tables_list[:5]
tables_dict = dict(zip(df.TABLE_NAME[:5], df.RAW_FILE_NAME[:5]))
# TODO COGER LA FECHA DEL DAG PARA PODER HACER EL BACKFILL
date = datetime.now().strftime('%y%m%d')  # 230222

doc_md = """
### lakehouse-ingestion-pipeline
#### Purpose
Load all the raw files from the defined sources (host, Flash, external sources...)
"""

for k, v in tables_dict.items():

    dag_id = f"table_{k}"
    param = f'{tables_dict[k]}'

    with DAG(
        dag_id='lakehouse-ingestion-pipeline',
        doc_md=doc_md,
        start_date=pendulum.datetime(2023, 2, 15, tz="UTC"),
        schedule=None,
        catchup=False,
        default_args={'retries': 3},
        tags=["example"],
    ) as dag:

        gcs_list_files = GCSListObjectsOperator(
            task_id='gcs_list_bucket',
            bucket='afb-input-reception',
            prefix=f"{date}/",
            delimiter='.csv',
            gcp_conn_id='google_cloud_default'
        )


        # def get_file(**kwargs):
        #     ti = kwargs['ti']
        #     all_files = ti.xcom_pull(task_ids='gcs_list_afb-input-reception', key='return_value')
        #     pattern = f"{tables_dict[k]}*"
        #     all_files = [f for f in all_files if re.search(pattern, f)]
        #
        #
        # logger.info('hola', extra={'labels': {'env': 'dev', 'tech': 'airflow_blanca'}})
        #
        # process_file = PythonOperator(
        #     task_id='file',
        #     python_callable=get_file,
        #     provide_context=True
        # )
        # all_files = "${{ task_instance.xcom_pull(task_ids='gcs_list_bucket', key='return_value') }}"
        # all_files = xcom_pull(task_ids='gcs_list_bucket', key='return_value')
        # logger.info(all_files, extra={'labels': {'print': 'all_files', 'tech': 'airflow_blanca'}})
        # logger.info('all_files: ', str(all_files))
        # pattern = f"{date}/{tables_dict[k]}*"
        # all_files = [f for f in all_files if re.search(pattern, f)]
        # my_file = 'Distribuidora_230222.csv'
        # my_file = all_files

        gcs_sensor = GCSObjectsWithPrefixExistenceSensor(
            task_id=f"gcs_sensor_{tables_dict[k]}",
            bucket="afb-input-reception",
            prefix=f"{date}",
            # object=f'230222/Distribuidora_230222.csv',
            # google_cloud_conn_id='google_cloud_default',
            # dag=dag
        )
        # print_token = BashOperator(
        #     task_id='print_token',
        #     bash_command='gcloud auth print-identity-token blanca.gonzalez@allfunds.com "--audiences=https://cloudrun-input-reception-ezi6y5dy7a-ew.a.run.app"'
        # )
        # token = "{{ task_instance.xcom_pull(task_ids='print_token') }}"  # gets output from 'print_token' task
        # logger.info(token, extra={'labels': {'print': 'token', 'tech': 'airflow_blanca'}})
        # task_get_op = SimpleHttpOperator(
        #     task_id='get_http_op',
        #     method='GET',
        #     http_conn_id='cloud_run',
        #     headers={'Authorization': 'Bearer ' + token},
        # )
        #
        #
        # def process_data_from_http(**kwargs):
        #     ti = kwargs['ti']
        #     http_data = ti.xcom_pull(task_ids='get_http_op')
        #     logger.info(http_data, extra={'labels': {'print': 'http_data', 'tech': 'airflow_blanca'}})
        #
        #
        # process_data = PythonOperator(
        #     task_id='process_data',
        #     python_callable=process_data_from_http,
        #     provide_context=True
        # )
        # gcs_list_files >> process_file >> gcs_sensor >> print_token >> task_get_op >> process_data
        # with dag:
        #     gcs_list_files >> process_file >> gcs_sensor >> print_token >> task_get_op >> process_data
