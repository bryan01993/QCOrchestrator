from datetime import datetime

from airflow import DAG
from airflow.providers.google.cloud.operators.dataform import (
    DataformCreateWorkflowInvocationOperator,
)
from airflow.providers.google.cloud.sensors.dataform import (
    DataformWorkflowInvocationStateSensor,
)
from airflow.utils.trigger_rule import TriggerRule
from google.cloud.dataform_v1beta1 import WorkflowInvocation

from includes.TemplatedDataformCreateCompilationResultOperator import (
    TemplatedDataformCreateCompilationResultOperator,
)

# Constants
TABLE_ID = "{{ table_id }}"
DAG_VERSION = "{{ dag_version }}"
STAGE = "{{ stage }}"

DW_PROJECT_ID = "{{ dw_project_id }}"
REPOSITORY_ID = "{{ repository_id }}"
REGION = "europe-west1"
GIT_COMMITISH = "{{ git_commitish }}"
EXECUTION_SEATING = "{{ execution_seating }}"
BRONZE_DB = "{{ ing_project_id }}"
DEFAULT_DATABASE = "{{ defaultDatabase }}"
TAGS_BASE = ["dataform", "SILVER"]

CONCURRENCY = 1
START_DATE = datetime(2017, 1, 1)
DAG_ID = f"allfunds_{TABLE_ID}_{STAGE}_{DAG_VERSION}"

# DAG Definition
with DAG(
    DAG_ID,
    schedule_interval=None,
    start_date=START_DATE,
    catchup=False,
    render_template_as_native_obj=True,
    tags=TAGS_BASE + [DAG_ID, EXECUTION_SEATING],
    concurrency=CONCURRENCY,
) as dag:
    create_compilation_result = TemplatedDataformCreateCompilationResultOperator(
        task_id="create_compilation_result",
        project_id=DW_PROJECT_ID,
        region=REGION,
        repository_id=REPOSITORY_ID,
        compilation_result={
            "git_commitish": GIT_COMMITISH,
            "code_compilation_config": {
                "default_database": DEFAULT_DATABASE,
                "vars": {
                    "executionSetting": EXECUTION_SEATING,
                    "bronzeDatabase": BRONZE_DB,
                    "operationsStartDate": "{% raw %}{{ dag_run.conf['min_load_batch'] }}{% endraw %}",
                },
            },
        },
        trigger_rule=TriggerRule.ALL_SUCCESS,
    )

    create_workflow_invocation = DataformCreateWorkflowInvocationOperator(
        task_id="create_workflow_invocation",
        project_id=DW_PROJECT_ID,
        region=REGION,
        repository_id=REPOSITORY_ID,
        asynchronous=True,
        workflow_invocation={
            "compilation_result": "{% raw %}{{ task_instance.xcom_pull('create_compilation_result')['name'] }}{% endraw %}",
            "invocation_config": {
                "included_tags": [TABLE_ID],
                "transitive_dependencies_included": False,
                "transitive_dependents_included": False,
                "fully_refresh_incremental_tables_enabled": False,
            },
        },
        trigger_rule=TriggerRule.ALL_SUCCESS,
    )

    is_workflow_invocation_done = DataformWorkflowInvocationStateSensor(
        task_id="is_workflow_invocation_done",
        project_id=DW_PROJECT_ID,
        region=REGION,
        retries=1,
        repository_id=REPOSITORY_ID,
        workflow_invocation_id="{% raw %}{{ task_instance.xcom_pull('create_workflow_invocation')['name'].split('/')[-1] }}{% endraw %}",
        expected_statuses={WorkflowInvocation.State.SUCCEEDED},
        failure_statuses={
            WorkflowInvocation.State.FAILED,
            WorkflowInvocation.State.CANCELLED,
        },
        trigger_rule=TriggerRule.ALL_SUCCESS,
    )

    # Task Dependencies
    (
        create_compilation_result
        >> create_workflow_invocation
        >> is_workflow_invocation_done
    )
