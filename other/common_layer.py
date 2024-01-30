# Copyright 2021 Google LLC. This software is provided as-is, without warranty
# or representation for any use or purpose. Your use of it is subject to your
# agreement with Google.
import google

from includes.allfunds.loggers import log_info, log_warning
from includes.allfunds.utils import get_gcs_dag_config_files, get_local_dag_config_files, parse_dag_configuration, prepare_batch_payload
# from includes.allfunds.utils import get_local_dag_config_files, parse_dag_configuration, prepare_batch_payload, \
#     removesuffix

from airflow import DAG

from airflow.models import Variable
from airflow.operators.empty import EmptyOperator
from airflow.providers.google.cloud.sensors.gcs import GCSObjectsWithPrefixExistenceSensor
# from astronomer.providers.google.cloud.sensors.gcs import GCSObjectsWithPrefixExistenceSensorAsync
from airflow.providers.google.cloud.transfers.gcs_to_gcs import GCSToGCSOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.google.cloud.operators.dataproc import DataprocCreateBatchOperator

from airflow.decorators import task
from datetime import datetime
from airflow.operators.python import get_current_context
import json
import os

credentials, project_id = google.auth.load_credentials_from_file(
    "/opt/airflow/dags/afb-lakehouse-poc-f00756f9a1a7.json")
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = "/opt/airflow/dags/afb-lakehouse-poc-f00756f9a1a7.json"


################################ Main Workflow steps below ##################################

def create_data_ingestion_dag(dag_id,
                              schedule,
                              default_args,
                              source_config,
                              processing_config,
                              destination_config
                              ):
    """Return new created DAG from given config
    Input :
    - dag_id string
    - schedule String
    - default_args dict
    - source_config dict
    Output : DAG
    """

    source_objects_prefix = source_config.source_objects_prefix
    landing_bucket = source_config.landing_bucket
    processing_bucket = processing_config.staging_bucket
    target_project_id = destination_config.target_project_id
    dataset_id = destination_config.dataset_name
    source_format = source_config.source_format
    table_name = destination_config.table_name
    # store_bucket = destination_config.store_bucket
    # staging_bucket = "whejna-allfunds-datalake"

    # override the start date with the right format
    if "start_date" in default_args:
        default_args["start_date"] = datetime.strptime(default_args.get("start_date"), '%d/%m/%y')

    dag = DAG(dag_id,
              schedule_interval=schedule,
              default_args=default_args,
              max_active_runs=5,
              catchup=False)

    with dag:
        start = EmptyOperator(
            task_id='start',
            dag=dag,
        )

        prefix = f"""{source_objects_prefix}/{source_objects_prefix}""" + "{{ ds_nodash }}"

        wait_for_data = GCSObjectsWithPrefixExistenceSensor(
            task_id=f"gcs_wait_for_data_files",
            # google_cloud_conn_id=Variable.get("allfunds_landing_bucket_conn_id"),
            bucket=landing_bucket,
            prefix=prefix,
            mode="reschedule",
            # TODO: fix and put to the configuration file
            poke_interval=300
        )

        @task(task_id="local_prepare_dataproc_input", multiple_outputs=True)
        def prepare_dataproc_input():
            pyspark_batch_args = {
                "input_bucket": f"{processing_bucket}/{source_objects_prefix}*",
                "template_name": "GCSTOBIGQUERY",
                "source_format": source_format,
                "output_mode": "overwrite",
                "dataset_id": "raw_data",
                "table_name": f"{table_name}_dataproc",
                "dataproc_staging_bucket": "whejna-allfunds-datalake",
                "dataproc_subnetwork_project_id": "whejna-allfunds-datalake",
                "dataproc_subnetwork_region_id": "europe-west1",
                "dataproc_subnetwork_name": "default",
                "dataproc_service_account": "dataproc-sa@whejna-allfunds-datalake.iam.gserviceaccount.com"
            }

            return prepare_batch_payload(**pyspark_batch_args)

        prepare_dataproc_input_op = prepare_dataproc_input()

        batch_ingestion_task = DataprocCreateBatchOperator(
            task_id="dataproc_transform",
            batch_id=f"ingestion-from-composer-{table_name.replace('_', '-')}-{int(datetime.timestamp(datetime.now()))}",
            project_id="whejna-allfunds-datalake",
            region="europe-west3",
            gcp_conn_id="allfunds_cloud_default",
            batch=prepare_dataproc_input_op
        )

        load_to_bq_op = GCSToBigQueryOperator(
            task_id='bigquery_load',
            bucket=processing_bucket,
            destination_project_dataset_table=f"{target_project_id}:{dataset_id}.{table_name}",
            source_format=source_format,
            source_objects=batch_ingestion_task.output,
            compression="NONE",
            create_disposition="CREATE_IF_NEEDED",
            write_disposition="WRITE_TRUNCATE",
            field_delimiter=",",
            skip_leading_rows=1,
            location="europe-west1",
            autodetect=True
        )

        end = EmptyOperator(
            task_id='end',
            dag=dag,
        )

        # define the dag dependencies
        # start >> wait_for_data >> move_to_processing >> load_to_bq_op >> move_to_store >> end
        start >> wait_for_data >> prepare_dataproc_input_op >> batch_ingestion_task >> load_to_bq_op >> end
        # move_to_processing >> prepare_dataproc_input_op >> batch_ingestion_task >> move_to_store

    log_info(msg=f'DAG {dag_id} is added')

    return dag


################################ Main Function ##################################

def main(config_path):
    """Parse the configuration and submit dynamic dags to airflow global namespace based on config files from GCS bucket.
    args:
    config_path - path of the config (bucket / local path) where the configuration file are
    """
    try:
        dag_config_files = get_gcs_dag_config_files(config_path)
    except:
        dag_config_files = get_local_dag_config_files(config_path)

    log_info(msg=f"added config: {dag_config_files}")

    ingestion_dags = parse_dag_configuration(dag_config_files)
    for dag in ingestion_dags:
        globals()[dag.dag_id] = create_data_ingestion_dag(dag.dag_id,
                                                          dag.schedule,
                                                          dag.default_args,
                                                          dag.source_config,
                                                          dag.processing_config,
                                                          dag.destination_config
                                                          )


# set up the relative path (to avoid using Airflow vars)

# config_path = "gs://afb-input-config-dev"
config_path = "/opt/airflow/plugins/config"

main(config_path)
