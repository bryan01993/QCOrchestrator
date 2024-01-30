
import pendulum
from airflow import DAG
from airflow.decorators import task
from airflow.models import Connection
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryGetDataOperator, BigQueryExecuteQueryOperator
from airflow.providers.google.cloud.operators.dataproc import DataprocCreateBatchOperator, DataprocSubmitJobOperator
from airflow.providers.google.cloud.sensors.gcs import GCSObjectsWithPrefixExistenceSensor
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.google.cloud.operators.gcs import GCSDeleteObjectsOperator
from airflow.providers.google.cloud.operators.pubsub import PubSubPublishMessageOperator
from google.cloud import logging as gcp_logging
from google.cloud.logging_v2.handlers import CloudLoggingHandler
import logging
import google.auth
import os
import json
from datetime import datetime
from google.cloud import bigquery


credentials, project_id = google.auth.load_credentials_from_file("/opt/airflow/dags/afb-lakehouse-poc-129e59b6fe9e.json")
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "/opt/airflow/dags/afb-lakehouse-poc-129e59b6fe9e.json"

client = gcp_logging.Client(project='afb-lakehouse-poc')
handler = CloudLoggingHandler(client)
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
logger.addHandler(handler)

def get_date(**context) -> str:
    # ti = context['ti']
    # var = get_file_config_from_bq.xcom_pull(context='ti', task_ids='GET_FILE_CONFIG' )

    return  str(datetime.now().strftime('%y%m%d'))

def success_callback_function(**context):
    dag_run = context.get('dag_run')
    task_instance = context.get('task_instance')
    log_msg = f'Task {task_instance.task_id} succeeded at {datetime.now()}'
    logger.info('success', extra={'labels': {'env': 'dev', 'tech': 'success_callback_executed', 'result': 'success_on_callback'}})
    dag_run.log.info(log_msg)

def log_failure_task(**context):
    dag_run = context.get('dag_run')
    task_instance = context.get('task_instance')
    log_msg = f'Task {task_instance.task_id} failed at {datetime.now()}'
    logger.info(log_msg)
    logger.info('failure', extra={'labels': {'env': 'dev', 'tech': 'failure_callback_executed', 'result': 'failure_on_callback'}})
    dag_run.log.info(log_msg)

# date = datetime.now().strftime('%y%m%d')

with DAG(
        dag_id='avro-to-bq-ingestion-backup',
        start_date=datetime(2023, 2, 20),
        schedule_interval=None,
        catchup=False,
        default_args={'retries': 3},
        on_success_callback=success_callback_function,
        on_failure_callback=log_failure_task,
        tags=["TAG-TESTER"]
) as dag:

    empty_sensor_op = EmptyOperator(task_id="empty_sensor_op", on_success_callback=success_callback_function)

    get_file_config_from_bq = BigQueryGetDataOperator(task_id='GET_FILE_CONFIG',
                                                      dataset_id='control_dev',
                                                      table_id='dl_conf_batch_exec_dev',
                                                      max_results=1000,
                                                      selected_fields='RAW_FILE_NAME,'
                                                                      'TABLE_NAME,'
                                                                      'DATASET,'
                                                                      'INGEST_MODE,'
                                                                      'COMMA_DELIMITER_TABLES,'
                                                                      'NO_HEADER_TABLES,'
                                                                      'NO_FOOTER_TABLES,'
                                                                      'NO_LAST_SEMICOLON,'
                                                                      'IND_SPLIT, IND_ACTIVE',
                                                      gcp_conn_id='google_cloud_default',
                                                      location='EU',
                                                      do_xcom_push=True,
                                                      on_success_callback=success_callback_function)
    #

    operator_python = PythonOperator(task_id="log_test",
                                     python_callable=success_callback_function)

    empty_sensor_op >> operator_python >> get_file_config_from_bq

    # reach_file_in_gcs = GCSObjectsWithPrefixExistenceSensor(task_id='REACH_FILE_GCS',
    #                                                         bucket='afb-input-reception',
    #                                                         prefix=f'{get_date()}/prueba_{get_date()}.csv')
    #
    # gcs_to_bigquery = GCSToBigQueryOperator(task_id='GCS_TO_BQ',
    #                                         # gcp_conn_id="google_cloud_default",
    #                                         bucket='test-input-dataflow',
    #                                         source_objects='operaciones_folder/Operaciones221125',
    #                                         source_format='AVRO',
    #                                         destination_project_dataset_table='afb-lakehouse-poc:pruebas_ingestas_multireg.avro_to_bq_airflow',
    #                                         create_disposition='CREATE_IF_NEEDED',
    #                                         write_disposition="WRITE_TRUNCATE",
    #                                         location='EU'
                                            # time_partitioning={
                                            #                     'field': 'S_COMMISSION_APPLIED',
                                            #                     'expiration_ms': 365,
                                            #                     'require_partition_filter': True
                                            #                     }
    #                                         )
    #
    # delete_storage_item = GCSDeleteObjectsOperator(task_id='CLEAN_STORAGE',
    #                                                bucket_name='test-input-dataflow',
    #                                                # prefix='operaciones_folder/',
    #                                                objects=['operaciones_folder/Operaciones221125'])
    #
    # var_json_encoded = json.dumps({"data": "ops_file"})
    # publish_message = PubSubPublishMessageOperator(task_id='PUBLISH_MESSAGE',
    #                                                project_id=project_id,
    #                                                topic='pruebas_blanca',
    #                                                messages=[{"data": b"ops_file"}])

    # empty_sensor_op # >> reach_file_in_gcs >> >> gcs_to_bigquery >> publish_message >> delete_storage_item


# docker cp dags/dl-etl-pipeline.py fc0a44767e515c8b3a5499f3b5caa0ecdc091b938deba7a716b02b710d384655:/opt/airflow/dags



