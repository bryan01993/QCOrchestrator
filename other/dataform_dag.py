#
# from datetime import datetime, timedelta
# from airflow import DAG
#
# from google.cloud.dataform_v1beta1 import WorkflowInvocation
# from airflow.providers.google.cloud.sensors.dataform import DataformWorkflowInvocationStateSensor
#
# from airflow.models.baseoperator import chain
# from airflow.providers.google.cloud.operators.dataform import (
#     DataformCreateCompilationResultOperator,
#     DataformCreateWorkflowInvocationOperator
# )
#
# DAG_ID = "dataform"
# PROJECT_ID = "whejna-allfunds-datalake"
# REPOSITORY_ID = "dataform-demo"
# REGION = "europe-west1"
# GIT_COMMITISH = "demo"
#
# with DAG(
#     DAG_ID,
#     schedule_interval='@once',  # Override to match your needs
#     start_date=datetime(2022, 1, 1),
#     catchup=False,  # Override to match your needs
#     tags=['dataform'],
# ) as dag:
#
#     create_compilation_result = DataformCreateCompilationResultOperator(
#         task_id="create_compilation_result",
#         project_id=PROJECT_ID,
#         region=REGION,
#         repository_id=REPOSITORY_ID,
#         compilation_result={
#             "git_commitish": GIT_COMMITISH,
#         },
#         gcp_conn_id="allfunds_cloud_default"
#     )
#
# create_workflow_invocation = DataformCreateWorkflowInvocationOperator(
#     task_id='create_workflow_invocation',
#     project_id=PROJECT_ID,
#     region=REGION,
#     repository_id=REPOSITORY_ID,
#     asynchronous=True,
#     workflow_invocation={
#         "compilation_result": "{{ task_instance.xcom_pull('create_compilation_result')['name'] }}"
#     },
#     gcp_conn_id="allfunds_cloud_default"
# )
#
# is_workflow_invocation_done = DataformWorkflowInvocationStateSensor(
#     task_id="is_workflow_invocation_done",
#     project_id=PROJECT_ID,
#     region=REGION,
#     repository_id=REPOSITORY_ID,
#     workflow_invocation_id=("{{ task_instance.xcom_pull('create_workflow_invocation')['name'].split('/')[-1] }}"),
#     expected_statuses={WorkflowInvocation.State.SUCCEEDED},
#     gcp_conn_id="allfunds_cloud_default"
# )
#
#
# create_compilation_result >> create_workflow_invocation >> is_workflow_invocation_done
