import pendulum
from airflow import DAG
from airflow.decorators import task
from airflow.models import Connection
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryGetDataOperator, BigQueryExecuteQueryOperator
from airflow.providers.google.cloud.sensors.gcs import GCSObjectsWithPrefixExistenceSensor
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.google.cloud.operators.gcs import GCSDeleteObjectsOperator
from airflow.providers.google.cloud.operators.pubsub import PubSubPublishMessageOperator
import uuid
from airflow.providers.google.cloud.operators.dataproc import DataprocCreateBatchOperator
from google.cloud import logging as gcp_logging
from google.cloud.logging_v2.handlers import CloudLoggingHandler
import logging
import google.auth
import os
import json
from datetime import datetime, timedelta
from google.cloud import bigquery

# Permisions
credentials, project_id = google.auth.load_credentials_from_file("/opt/airflow/dags/gcp-automation.json")
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "/opt/airflow/dags/gcp-automation.json"

# Logging Handlers
client = gcp_logging.Client(project='afb-lakehouse-poc')
handler = CloudLoggingHandler(client)
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
logger.addHandler(handler)


def get_date(**context) -> str:
    return str(datetime.now().strftime('%y%m%d'))


def success_callback_function(**context):
    dag_run = context.get('dag_run')
    task_instance = context.get('task_instance')
    log_msg = f'Task {task_instance.task_id} succeeded at {datetime.now()}'
    logger.info('success',
                extra={'labels': {'env': 'dev', 'tech': 'success_callback_executed', 'result': 'success_on_callback'}})
    dag_run.log.info(log_msg)


def log_failure_task(**context):
    dag_run = context.get('dag_run')
    task_instance = context.get('task_instance')
    log_msg = f'Task {task_instance.task_id} failed at {datetime.now()}'
    logger.info(log_msg)
    logger.info('failure',
                extra={'labels': {'env': 'dev', 'tech': 'failure_callback_executed', 'result': 'failure_on_callback'}})
    dag_run.log.info(log_msg)


@task(task_id="local_prepare_dataproc_input", multiple_outputs=True)
def get_job_parameters(**kwargs):
    job_parameters = {
        'main_py_file_uri': 'gs://afb-input-reception-dev/test_pyspark_files/csv_to_avro_transforms.py',  # hardcoded
        'version': '2.0',  # hardcoded
        'container_image': 'eu.gcr.io/afb-lakehouse-poc/spark-avro-job:1.0.4',  # hardcoded
        'files': 'gs://afb-input-reception-dev/test_pyspark_files/conf',  # hardcoded
        'py_files': 'gs://afb-input-reception-dev/test_pyspark_files/logging_decorator.py',  # hardcoded
        'subnet': 'default',  # hardcoded
        'table_id': 'operations',  # PARAMETER
        'schema_path': 'operations.json',  # PARAMETER
        'file_path': 'gs://afb-input-reception-dev/test_files/Operaciones230215.csv',  # PARAMETER
        'delimiter': ';'  # PARAMETER
    }
    # kwargs['ti'].xcom_push(key='test', value=job_parameters)
    return job_parameters


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 2, 22),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

with DAG(
        dag_id='avro-to-bq-full',
        schedule_interval=timedelta(days=1),
        catchup=False,
        default_args=default_args,
        on_success_callback=success_callback_function,
        on_failure_callback=log_failure_task,
        tags=["TAG-TESTER"]
) as dag:
    job_custom_parameters = get_job_parameters()
    execution_date = "{{ ds_nodash }}"  # AAAAMMDD E.G: 20230223
    job_uuid = str(uuid.uuid4()).split("-")[0]
    job_name = f"{job_custom_parameters['table_id']}-{job_uuid}-{execution_date}"

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

    create_batch = DataprocCreateBatchOperator(
        task_id="dataproc_custom_container",
        project_id='afb-lakehouse-poc',
        region='europe-west1',
        batch={
            "pyspark_batch": {
                "python_file_uris": [
                    job_custom_parameters['py_files']
                ],
                "jar_file_uris": [],
                "file_uris": [
                    job_custom_parameters['files']
                ],
                "args": [
                    f"--table_id={job_custom_parameters['table_id']}",
                    f"--schema_path={job_custom_parameters['schema_path']}",
                    f"--file_path={job_custom_parameters['file_path']}",
                    f"--delimiter={job_custom_parameters['delimiter']}"
                ],
                "main_python_file_uri": job_custom_parameters['main_py_file_uri']
            },
            "labels": {
                "env": "dev",
                "job_type": "dataproc_template"
            },
            "runtime_config": {
                "version": job_custom_parameters['version'],
                "container_image": job_custom_parameters['container_image'],
                "properties": {}
            },
            "environment_config": {
                "execution_config": {
                    "subnetwork_uri": job_custom_parameters['subnet']
                }
            }
        },
        batch_id=f"{job_name}"
    )
    operator_python = PythonOperator(task_id="log_test",
                                     python_callable=success_callback_function)

    # wait_for_data = GCSObjectsWithPrefixExistenceSensor(task_id=f"gcs_wait_for_data_files",
    #                                                     google_cloud_conn_id=Variable.get(
    #                                                         "allfunds_landing_bucket_conn_id"),
    #                                                     bucket=landing_bucket,
    #                                                     prefix=f"""{source_objects_prefix}""" + "{{ ds_nodash }}",
    #                                                     mode="reschedule",
    #                                                     # TODO: fix and put to the configuration file
    #                                                     poke_interval=300)

    gcs_to_bigquery = GCSToBigQueryOperator(task_id='GCS_TO_BQ',
                                            # gcp_conn_id="google_cloud_default",
                                            bucket='test-input-dataflow',
                                            source_objects='operaciones_folder/Operaciones221125',
                                            source_format='AVRO',
                                            destination_project_dataset_table='afb-lakehouse-poc:pruebas_ingestas_multireg.avro_to_bq_airflow',
                                            create_disposition='CREATE_IF_NEEDED',
                                            write_disposition="WRITE_TRUNCATE",
                                            location='EU',
                                            time_partitioning={
                                                'field': 'S_COMMISSION_APPLIED',
                                                'expiration_ms': 365,
                                                'require_partition_filter': True
                                            }
                                            )

    delete_storage_item = GCSDeleteObjectsOperator(task_id='CLEAN_STORAGE',
                                                   bucket_name='test-input-dataflow',
                                                   # prefix='operaciones_folder/',
                                                   objects=['operaciones_folder/Operaciones221125'])

    publish_message = PubSubPublishMessageOperator(task_id='PUBLISH_MESSAGE',
                                                   project_id=project_id,
                                                   topic='pruebas_blanca',
                                                   messages=[{"data": b"ops_file"}])

    empty_sensor_op  >> create_batch >> gcs_to_bigquery >> publish_message >> delete_storage_item

# docker cp dags/dl-etl-pipeline.py fc0a44767e515c8b3a5499f3b5caa0ecdc091b938deba7a716b02b710d384655:/opt/airflow/dags
