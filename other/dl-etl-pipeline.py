import random
from textwrap import dedent

import pendulum
from airflow import DAG
from airflow.decorators import task
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator

doc_md = """
### lakehouse-ingestion-pipeline
#### Purpose
Load all the raw files from the defined sources (host, Flash, external sources...)
"""

with DAG(
        dag_id='lakehouse-ingestion-pipeline',
        doc_md=doc_md,
        start_date=pendulum.datetime(2023, 2, 15, tz="UTC"),
        schedule_interval=None,
        catchup=False,
        default_args={'retries': 3},
        tags=["example"]
) as dag:
    doc_md_sensor = """
        ## sensor
        #### Purpose
        ##### The motive of this task is to detect when a file arrives to the GCS bucket and trigger the DAG
        ---        
    """


    @task.python(task_id="sensor", doc_md=doc_md_sensor)
    def sensor_func(**context):
        ti = context['ti']
        descomprimir = random.choice[True, False]
        ti.xcom_push(key='descomprimir', value=descomprimir)


    doc_md_branch = """
        ## branch_func
        #### Purpose
        ##### The purpose of this task is to decide whether it is necessary to decompress the file or not.
        If it is needed, the Airflow line follows the Cloud Run path to decompress it. If not, the information is sent directly to Spark (Dataproc).
        ---        
    """


    @task.branch(task_id="branch", doc_md=doc_md_branch)
    def branch_func(**context):
        xcom_value = context['ti'].xcom_pull(task_ids="sensor", key='descomprimir')
        print(xcom_value)
        if xcom_value:
            return "extract_cloudrun"
        elif not xcom_value:
            return "no_unzip"
        else:
            return None


    doc_md_cloudrun = """
        ## extract_cloudrun
        #### Purpose
        ##### This task is responsible for calling the Cloud Run and passing the ingest information to it via XCOM. In this step it is decompressed and some basic checks are made.
        ---        
    """

    cloudrun_op = BashOperator(task_id="extract_cloudrun", bash_command="sleep 5", doc_md=doc_md_cloudrun)

    doc_md_dataproc = """
        ## transform_spark
        #### Purpose
        ##### The task is sent to a DataprocOperator so that Spark processes the file, applies the necessary transformations and leaves an AVRO file as a result.
        ---        
    """

    dataproc_op = EmptyOperator(task_id="transform_spark", doc_md=doc_md_dataproc)

    doc_md_load_bq = """
        ## load_bq
        #### Purpose
        ##### In this task we load the AVRO that we have previously generated in BigQuery with a BigqueryOperator
        ---        
    """

    bigquery_op = EmptyOperator(task_id="load_bq", doc_md=doc_md_load_bq)

    doc_md_pubsub = """
        ## publish_pubsub
        #### Purpose
        ##### To indicate that the table has been loaded correctly, we send a PubSub message with the information of the loaded table that can be used by other services
        ---        
    """

    pubsub_op = EmptyOperator(task_id="publish_pubsub", doc_md=doc_md_pubsub)

    start_op = sensor_func()
    branch_op = branch_func()

    start_op >> branch_op
    branch_op >> cloudrun_op >> dataproc_op
    branch_op >> dataproc_op
    dataproc_op >> bigquery_op >> pubsub_op

# docker cp dags/dl-etl-pipeline.py 17bf1e990ade39767f3efb9b528284fc1cdaed27ded3071e43f88ba034bd8626:/opt/airflow/dags
