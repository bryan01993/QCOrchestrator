import logging
import os
import sys
from typing import List

from google.cloud import bigquery
from jinja2 import Environment, FileSystemLoader, select_autoescape, Template

logging.basicConfig(level=logging.INFO)

DW_PROJECT_ID = os.getenv("PROJECT_ID_DATAWAREHOUSE")


def run_bigquery_query(client: bigquery.Client, query: str) -> bigquery.QueryJob:
    """
    Run a SQL query using BigQuery.

    :param client: BigQuery client
    :param query: SQL query to run
    :return: bigquery.QueryJob with results
    """
    try:
        query_job: bigquery.QueryJob = client.query(query)
        return query_job
    except Exception as e:
        raise Exception(f"An error occurred while querying BigQuery: {e}")


def generate_dag_files(
    template: Template,
    active_table_names: List[str],
    output_dir: str,
    dag_version: str,
    stage: str,
    dw_project_id: str,
    repository_id: str,
    git_commitish: str,
    execution_seating: str,
    ing_project_id: str,
    defaultDatabase: str,
) -> None:
    """
    Generate DAG files using the provided Jinja2 template.


    :param template: Jinja2 template for the DAG
    :param active_table_names: List of active table names
    :param stage: Dag medal system stage reference STR (EX. SILVER)
    :param dag_version:  Version of the dag STR (EX. V1)
    :param output_dir: Output directory to save generated DAG files
    """
    error_file_path = f"{output_dir}render_errors.txt"

    for table_id in active_table_names:
        try:
            rendered_dag = template.render(
                table_id=table_id,
                dag_version=dag_version,
                stage=stage,
                dw_project_id=dw_project_id,
                repository_id=repository_id,
                git_commitish=git_commitish,
                execution_seating=execution_seating,
                ing_project_id=ing_project_id,
                defaultDatabase=defaultDatabase,
            )
            with open(
                f"{output_dir}allfunds_{table_id}_{stage}_{dag_version}.py", "w"
            ) as f:
                f.write(rendered_dag)
            logging.info(f"Successfully generated DAG for table {table_id}")
        except Exception as e:
            error_message = f"An error occurred while writing the DAG file for table {table_id}: {e}"
            logging.error(error_message)
            with open(error_file_path, "a") as error_file:
                error_file.write(f"{error_message}\n")


if __name__ == "__main__":
    ENV = sys.argv[1]

    assert ENV in ["pro", "pre", "dev"], "invalid environment"

    # Constants
    DAG_VERSION = "V1"
    STAGE = "SILVER"
    REPOSITORY_ID = f"afb-lh-dataform-{ENV}"
    GIT_COMMITISH = "develop"
    if ENV == "pro":
        GIT_COMMITISH = "master"
    elif ENV == "pre":
        GIT_COMMITISH = "pre"
    EXECUTION_SEATING = ENV
    BRONZE_DB = f"afb-lh-ing-{ENV}"
    DEFAULT_DATABASE = f"afb-lh-dw-{ENV}"

    client = bigquery.Client(project=DW_PROJECT_ID)
    query = f"""
    SELECT TABLE_NAME as table_name, IND_ACTIVE as active
    FROM `afb-lh-orch-{ENV}.control.lh_ingestion_config`
    WHERE IND_ACTIVE = TRUE
    """

    logging.info("Running BigQuery query to get active table names.")
    active_table_names = [row.table_name for row in run_bigquery_query(client, query)]

    # DAG Template Configuration
    dag_template_path = "dataform_template.py"
    os.makedirs("dataform_dags", exist_ok=True)
    output_dir = "dataform_dags/"

    # Initialize Jinja2 Environment
    base_dir = os.path.dirname(os.path.abspath(__file__))
    template_dir = os.path.join(base_dir, "templates")
    env = Environment(
        loader=FileSystemLoader(searchpath=template_dir),
        autoescape=select_autoescape(["html", "xml"]),
    )

    logging.info("Loading Jinja2 template.")
    try:
        template = env.get_template(dag_template_path)
    except Exception as e:
        logging.error(f"An error occurred while getting the template: {e}")
        exit(1)

    logging.info("Generating DAG files.")
    generate_dag_files(
        template=template,
        active_table_names=active_table_names,
        output_dir=output_dir,
        dag_version=DAG_VERSION,
        stage=STAGE,
        dw_project_id=DW_PROJECT_ID,
        repository_id=REPOSITORY_ID,
        git_commitish=GIT_COMMITISH,
        execution_seating=EXECUTION_SEATING,
        ing_project_id=BRONZE_DB,
        defaultDatabase=DEFAULT_DATABASE,
    )
