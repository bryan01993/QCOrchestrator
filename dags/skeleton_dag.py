# Copyright 2021 Google LLC. This software is provided as-is, without warranty
# or representation for any use or purpose. Your use of it is subject to your
# agreement with Google.
import json
import os
import re
from datetime import datetime
from datetime import timedelta

from airflow import DAG, Dataset
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import BranchPythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.providers.google.cloud.operators.dataflow import (
    DataflowTemplatedJobStartOperator,
)
from airflow.providers.google.cloud.operators.dataproc import (
    DataprocCreateBatchOperator,
    DataprocDeleteBatchOperator,
)
from airflow.providers.google.cloud.operators.gcs import GCSDeleteObjectsOperator
from airflow.providers.google.cloud.transfers.gcs_to_gcs import GCSToGCSOperator
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.utils.trigger_rule import TriggerRule

from config.configuration import GCP, DABCONS_TABLES
from includes.allfunds.loggers import log_info
from includes.allfunds.utils import (
    get_gcs_dag_config_files,
    get_local_dag_config_files,
    parse_dag_configuration,
)
from includes.header_validator_operator import HeaderValidatorOperator
from includes.sharded_partitioned_gcstobigquery_operator import (
    ShardedPartitionedGCSToBigQueryOperator,
)
from includes.xcom_pusher import xcom_pusher

SILVER_DAG_VERSION = "V1"


################################ Main Workflow steps below ##################################
def create_data_ingestion_dag(
    dag_id,
    schedule,
    default_args,
    source_config,
    processing_config,
    destination_config,
    file_config,
):
    """Return new created DAG from given config
    Input :
    - dag_id string
    - schedule String
    - default_args dict
    - source_config dict
    Output : DAG
    """

    source = source_config.source
    responsible = source_config.responsible

    target_project_id: str = destination_config.target_project_id
    table_name: str = destination_config.table_name

    # override the start date with the right format
    if "start_date" in default_args:
        default_args["start_date"] = datetime.strptime(
            default_args.get("start_date"), "%d/%m/%y"
        )
    dag = DAG(
        dag_id,
        schedule_interval=schedule,
        default_args=default_args,
        max_active_runs=10,
        catchup=False,
        dagrun_timeout=timedelta(minutes=60),
        tags=[source, responsible],
    )

    with dag:
        start = EmptyOperator(
            task_id="start",
            dag=dag,
        )

        create_auth_token = BashOperator(
            task_id="create_auth_token",
            bash_command=f'gcloud auth print-identity-token "--audiences={os.getenv("CLOUD_RUN_URL")}"',
        )
        token = "{{ task_instance.xcom_pull(task_ids='create_auth_token') }}"  # gets output from 'print_token' task

        def branch_func(**kwargs):
            ti = kwargs["ti"]
            staging_file_path = ti.xcom_pull(key="file_path")
            if (
                any(
                    [
                        file_extension in staging_file_path
                        for file_extension in ["csv", "txt", "dat"]
                    ]
                )
                and ti.xcom_pull(key="table_name") not in DABCONS_TABLES
            ):
                if (
                    "zip" in staging_file_path
                    and ti.xcom_pull(key="table_name") not in DABCONS_TABLES
                ):
                    return [
                        "decompress",
                        "remove_header_footer",
                        "dataproc_custom_container",
                        "gcs_to_bq",
                        "clean_storage",
                        "end",
                    ]
                else:
                    return [
                        "copy_uncompressed_file",
                        "remove_header_footer",
                        "dataproc_custom_container",
                        "gcs_to_bq",
                        "clean_storage",
                        "end",
                    ]
            else:
                return [
                    "create_auth_token",
                    "cr_endpoint_call",
                    "gcs_to_bq",
                    "clean_storage",
                    "end",
                ]

        branch_op = BranchPythonOperator(
            task_id="cloudrun_or_bash",
            provide_context=True,
            python_callable=branch_func,
        )

        copy_uncompressed_file_op = GCSToGCSOperator(
            task_id="copy_uncompressed_file",
            source_bucket="{{ti.xcom_pull(key='staging_bucket')}}",
            source_object="{{ti.xcom_pull(key='file_path')}}",
            destination_bucket="{{ti.xcom_pull(key='processing_bucket')}}",
            destination_object="{{ ti.xcom_pull(key='file_date') }}/"
            "{{ti.xcom_pull(key='file_path_clean_uncompressed')}}{{ti.xcom_pull(key='file_extension')}}",
        )

        decompress_op = DataflowTemplatedJobStartOperator(
            task_id="decompress",
            project_id=target_project_id,
            location="europe-west1",
            template="gs://dataflow-templates/latest/Bulk_Decompress_GCS_Files",
            dataflow_default_options={
                "tempLocation": 'gs://{{ti.xcom_pull(key="dataproc_staging_bucket")}}/staging/',
                "ipConfiguration": "WORKER_IP_PRIVATE",
                "subnetwork": f"https://www.googleapis.com/compute/v1/projects/afb-lh-ing-{os.environ['DEPLOYMENT_TAG']}/regions/europe-west1/subnetworks/default",
            },
            parameters={
                "inputFilePattern": "gs://{{ti.xcom_pull(key=\"staging_bucket\")}}/{{ti.xcom_pull(key='file_path')}}",
                "outputDirectory": "gs://{{ti.xcom_pull(key=\"processing_bucket\")}}/{{ ti.xcom_pull(key='file_date') }}",
                "outputFailureFile": "gs://{{ti.xcom_pull(key=\"processing_bucket\")}}/{{ ti.xcom_pull(key='source_objects_prefix') }}{{ ti.xcom_pull(key='file_date') }}-error.csv",
            },
        )

        header_validator_task = HeaderValidatorOperator(
            task_id="header_validator_task",
            file_path="{{ ti.xcom_pull(key='file_uncompressed_path') }}",
            schema_columns="{{ ti.xcom_pull(key='schema_columns') }}",
            n_header="{{ ti.xcom_pull(key='n_header') }}",
            delimiter="{{ ti.xcom_pull(key='delimiter') }}",
            trigger_rule=TriggerRule.ONE_SUCCESS,
            dag=dag,
        )

        def decide_path_based_on_header_footer(**kwargs):
            ti = kwargs["ti"]
            n_header = ti.xcom_pull(key="n_header")
            n_footer = ti.xcom_pull(key="n_footer")
            log_info(msg=f"n_header= {n_header} and n_footer= {n_footer}")
            if int(n_header) == 0 and int(n_footer) == 0:
                return "dataproc_custom_container"
            else:
                return "remove_header_footer"

        branch_on_header_footer_presence_op = BranchPythonOperator(
            provide_context=True,
            task_id="branch_on_header_footer_presence_op",
            python_callable=decide_path_based_on_header_footer,
            trigger_rule=TriggerRule.ONE_SUCCESS,
            dag=dag,
        )

        reformat_op = BashOperator(
            task_id="remove_header_footer",
            bash_command="gsutil cp {{ ti.xcom_pull(key='file_uncompressed_path') }} mytemporal.csv && "
            "tail -n +{{ ti.xcom_pull(key='n_header') }} mytemporal.csv > temp_file.csv && head -n -{{ ti.xcom_pull(key='n_footer') }} temp_file.csv > output_file.csv && rm temp_file.csv && rm mytemporal.csv && "
            "gsutil cp output_file.csv {{ ti.xcom_pull(key='file_uncompressed_path') }}",
        )

        cr_endpoint_call = SimpleHttpOperator(
            task_id="cr_endpoint_call",
            endpoint="input_reception",
            method="POST",
            data=json.dumps(
                {
                    "name": "{{ti.xcom_pull(key='file_path')}}",
                    "output_path": "{{ti.xcom_pull(key='pyspark_output_path')}}",
                }
            ),
            headers={
                "Content-Type": "application/json",
                "Authorization": "Bearer "
                + "{{ ti.xcom_pull(task_ids='create_auth_token') }}",
            },
            response_filter=lambda response: response.json()["file_uncompressed_path"],
            log_response=True,
            dag=dag,
            do_xcom_push=True,
        )

        create_batch = DataprocCreateBatchOperator(
            task_id="dataproc_custom_container",
            project_id="{{ ti.xcom_pull(key='target_project_id') }}",
            region="europe-west1",
            trigger_rule=TriggerRule.ALL_DONE,
            batch={
                "pyspark_batch": {
                    "python_file_uris": ["{{ ti.xcom_pull(key='pyspark_py_files') }}"],
                    "jar_file_uris": ["{{ ti.xcom_pull(key='pyspark_jar_files') }}"],
                    "file_uris": [],
                    "args": [
                        "--table_id={{ ti.xcom_pull(key='table_name') }}",
                        "--schema_path={{ ti.xcom_pull(key='source_schema_file_name') }}",
                        "--file_path={{ ti.xcom_pull(key='file_uncompressed_path') }}",
                        "--output_path="
                        + str("{{ ti.xcom_pull(key='pyspark_output_path') }}"),
                        "--load_batch=" + str("{{ ti.xcom_pull(key='file_date') }}"),
                        "--file_part=" + "{{ ti.xcom_pull(key='file_part') }}",
                        "--delimiter={{ ti.xcom_pull(key='delimiter') }}",
                        "--fixed_widths={{ ti.xcom_pull(key='fixed_widths') }}",
                        "--columns_format={{ ti.xcom_pull(key='columns_format') }}",
                        "--env=" + f"{os.environ['DEPLOYMENT_TAG']}",
                        "--output_format=parquet",
                    ],
                    "main_python_file_uri": "{{ ti.xcom_pull(key='pyspark_main_py_file_uri') }}",
                },
                "labels": {
                    "env": f"{os.environ['DEPLOYMENT_TAG']}",
                    "job_type": "dataproc_template",
                },
                "runtime_config": {
                    "version": "{{ ti.xcom_pull(key='pyspark_version') }}",
                    "properties": {},
                },
                "environment_config": {
                    "execution_config": {
                        "subnetwork_uri": "{{ ti.xcom_pull(key='pyspark_subnet') }}"
                    }
                },
            },
            batch_id="{{ ti.xcom_pull(key='pyspark_job_name') }}",
        )

        labels_dict = {"source": source_config.source}
        time_partitioning = {
            "type": "DAY",
            "field": "LOAD_BATCH",
            # 'expiration_ms': 2592000000 -> set expiration 30 days
        }
        gcs_to_bigquery = ShardedPartitionedGCSToBigQueryOperator(
            task_id="gcs_to_bq",
            # gcp_conn_id="google_cloud_default",
            bucket="{{ ti.xcom_pull(key='dataproc_bucket') }}",
            source_objects="{{ ti.xcom_pull(key='dataproc_bucket_object') }}",
            source_format="PARQUET",
            destination_project_dataset_table="{{ ti.xcom_pull(key='destination_project_dataset_table') }}",
            create_disposition="CREATE_IF_NEEDED",
            write_disposition="WRITE_TRUNCATE",
            location="EU",
            time_partitioning=time_partitioning,
            impersonation_chain=f"afb-lh-bigquery@afb-lh-ing-{os.environ['DEPLOYMENT_TAG']}.iam.gserviceaccount.com",
            labels=labels_dict,
            trigger_rule=TriggerRule.ONE_SUCCESS,
            file_part="{{ ti.xcom_pull(key='file_part') }}",
            file_uncompressed_path="{{ ti.xcom_pull(key='file_uncompressed_path') }}",
        )

        delete_storage_item = GCSDeleteObjectsOperator(
            task_id="clean_storage",
            bucket_name="{{ ti.xcom_pull(key='dataproc_bucket') }}",
            prefix="{{ ti.xcom_pull(key='dataproc_tmp_bucket') }}",
        )

        end = EmptyOperator(
            task_id="end", outlets=[Dataset("s3://bronze/" + table_name)], dag=dag
        )

        trigger_silver = TriggerDagRunOperator(
            task_id="trigger_silver",
            trigger_dag_id=f"allfunds_{table_name}_SILVER_{SILVER_DAG_VERSION}",
            trigger_run_id=f"{table_name}_SILVER_"
            + str("{{ ti.xcom_pull(key='load_batch') }}"),
            conf={"min_load_batch": str("{{ ti.xcom_pull(key='load_batch') }}")},
            execution_date="{{ ds }}",
            reset_dag_run=True,
            wait_for_completion=False,
        )

        (
            start
            >> xcom_pusher(
                dag_id,
                source_config,
                destination_config,
                processing_config,
                file_config,
            )
            >> branch_op
        )
        # especial files
        branch_op >> create_auth_token >> cr_endpoint_call >> gcs_to_bigquery

        # uncompressed files
        (branch_op >> copy_uncompressed_file_op >> header_validator_task)

        # compressed files
        (branch_op >> decompress_op >> header_validator_task)

        (header_validator_task >> branch_on_header_footer_presence_op)

        # bifurcation by footer and/or header
        (
            branch_on_header_footer_presence_op
            >> reformat_op
            >> create_batch
        )
        branch_on_header_footer_presence_op >> create_batch

        # upload file, clean and trigger
        create_batch >> gcs_to_bigquery
        gcs_to_bigquery >> delete_storage_item >> trigger_silver >> end

    log_info(msg=f"DAG {dag_id} is added")

    return dag


################################ Main Function ##################################


def main():
    """Parse the configuration and submit dynamic dags to airflow global namespace based on config files from GCS bucket.
    args:
    config_path - path of the config (bucket / local path) where the configuration file are
    """
    try:
        log_info(msg=f"Reading path: {GCP.DAG.CONFIG.FOLDER_PATH_GCS}")
        dag_config_files = get_gcs_dag_config_files(GCP.DAG.CONFIG.FOLDER_PATH_GCS)
    except:
        log_info(msg=f"{GCP.DAG.CONFIG.FOLDER_PATH_GCS} path not found - trying backup")
        log_info(msg=f"Reading path: {GCP.DAG.CONFIG.FOLDER_PATH_LOCAL}")
        dag_config_files = get_local_dag_config_files(GCP.DAG.CONFIG.FOLDER_PATH_LOCAL)

    log_info(msg=f"added config: {dag_config_files}")

    ingestion_dags = parse_dag_configuration(dag_config_files)

    for dag in ingestion_dags:
        globals()[dag.dag_id] = create_data_ingestion_dag(
            dag.dag_id,
            None,
            dag.default_args,
            dag.source_config,
            dag.processing_config,
            dag.destination_config,
            dag.file_config,
        )


main()
