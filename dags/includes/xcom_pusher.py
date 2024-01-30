import json
import re
import time
from datetime import datetime

from airflow.decorators import task
from airflow.operators.python import get_current_context

from config.configuration import allowed_extensions, allowed_extensions_zip
from google.cloud import storage


@task.python(multiple_outputs=True)
def xcom_pusher(
    dag_id, source_config, destination_config, processing_config, file_config, **kwargs
):
    source_objects_prefix = source_config.source_objects_prefix
    landing_bucket = source_config.landing_bucket
    staging_bucket = source_config.staging_bucket
    dataproc_bucket = source_config.dataproc_bucket
    schemas_bucket = source_config.schemas_bucket
    processing_bucket = processing_config.processing_bucket
    dataproc_staging_bucket = processing_config.dataproc_staging_bucket
    source_format = source_config.source_format
    source = source_config.source
    responsible = source_config.responsible
    ingest_mode = destination_config.ingest_mode
    target_project_id = destination_config.target_project_id
    dataset_name = destination_config.dataset_name
    table_name = destination_config.table_name
    delimiter = file_config.delimiter
    n_header = file_config.n_header
    n_footer = file_config.n_footer
    source_schema_file_name = source_config.source_schema_file_name
    last_semicolon = file_config.last_semicolon
    split = file_config.split
    fixed_widths = file_config.fixed_widths
    columns_format = file_config.columns_format
    context = get_current_context()
    # logical_date = context["ds_nodash"]
    # logical_date = logical_date.format()
    file_path = context["dag_run"].conf["file_path"]
    file_date = context["dag_run"].conf["file_date"]
    file_part = (
        context["dag_run"].conf["file_part"]
        if "file_part" in context["dag_run"].conf
        and context["dag_run"].conf["file_part"] != ""
        and context["dag_run"].conf["file_part"] is not None
        else 0
    )
    load_batch = datetime.strptime(file_date, "%Y%m%d").strftime("%Y-%m-%d")

    file_path_clean = file_path

    extensions = []
    extensions_zip = []

    for extension in allowed_extensions:
        if extension in file_path_clean:
            extensions.append(extension)
            file_path_clean = file_path_clean.replace(extension, "")
    for extension in allowed_extensions_zip:
        if extension in file_path_clean:
            extensions_zip.append(extension)
            file_path_clean = file_path_clean.replace(extension, "")

    file_extension = extensions[0]
    file_extension_zip = ""

    if extensions_zip:
        file_extension_zip = extensions_zip[0]

    file_suffix = f"{file_date}_part_{file_part}" if "part" in file_path else file_date
    partition_time = "DAY"

    prefix = f"""{source_objects_prefix}/{source_objects_prefix}"""
    lower_source_object = source_objects_prefix.lower()
    # base_job_name = (
    #     f"{re.sub(r'[^a-z0-9-]', '', lower_source_object)}-{file_date}-".lower()
    # )
    job_name = generate_dataproc_job_name(lower_source_object, file_date, file_part)
    print(job_name)

    pyspark_input_file = f"gs://{processing_bucket}/{prefix}{file_date}.csv"
    pyspark_output_path = f"gs://{dataproc_bucket}/tmp/{table_name}/{job_name}"

    source_schema_file_name = f"gs://{schemas_bucket}/{source_schema_file_name}"
    schema_data, schema_columns = get_schema_from_file(
        schema_file_path=source_schema_file_name
    )

    return {
        "file_path": file_path,
        "file_date": file_date,
        "load_batch": load_batch,
        "file_part": file_part,
        "fixed_widths": fixed_widths,
        "columns_format": columns_format,
        "schema_data": schema_data,
        "schema_columns": schema_columns,
        "file_path_clean": file_path_clean,
        "file_path_clean_uncompressed": str(file_path_clean).rsplit("/")[1],
        "file_extension": file_extension,
        "file_extension_zip": file_extension_zip,
        "file_uncompressed_path": f"gs://{'/'.join([processing_bucket, file_date, source_objects_prefix + file_suffix + file_extension])}",
        "partition_time": partition_time,
        "landing_bucket": landing_bucket,
        "staging_bucket": staging_bucket,
        "processing_bucket": processing_bucket,
        "dataproc_bucket": dataproc_bucket,
        "dataproc_staging_bucket": dataproc_staging_bucket,
        "dataproc_bucket_object": f"tmp/{table_name}/{job_name}/part*",
        "dataproc_tmp_bucket": f"tmp/{table_name}/{job_name}",
        "ingest_mode": ingest_mode,
        "target_project_id": target_project_id,
        "source_objects_prefix": source_objects_prefix,
        "source_format": source_format,
        "source": source,
        "pyspark_py_files": f"gs://{dataproc_bucket}/config/logging_decorator.py".replace(
            "'", ""
        ),
        "pyspark_jar_files": f"gs://{dataproc_bucket}/config/gcs-connector-hadoop3-latest.jar".replace(
            "'", ""
        ),
        "pyspark_input_file": pyspark_input_file,
        "pyspark_output_path": pyspark_output_path,
        "pyspark_main_py_file_uri": f"gs://{dataproc_bucket}/config/dataproc_bronze_transform.py",
        "pyspark_job_name": job_name,
        "dag_id_value": dag_id,
        "pyspark_subnet": "default",
        "pyspark_version": "2.0",
        "responsible": responsible,
        "dataset_name": dataset_name,
        "table_name": table_name,
        "delimiter": delimiter,
        "n_header": int(n_header) + 1 if n_header > 0 else 0,
        "n_footer": int(n_footer),
        "source_schema_file_name": source_schema_file_name,
        "last_semicolon": last_semicolon,
        "split": split,
        "labels_dict": {"source": source_config.source},
        "destination_project_dataset_table": f"{target_project_id}:{dataset_name}.{table_name}${file_date}"
        if "full" not in ingest_mode
        else f"{target_project_id}:{dataset_name}.{table_name}",
    }


def generate_dataproc_job_name(
    source_object_name: str,
    file_date_str: str,
    file_part_str: str = "0",
) -> str:
    """
    Attempts to generate a valid job name for Dataproc that adheres to specific character and length constraints.
    Also ensures that the names are unique over table_name, file_date, and file_part.
    Adds a Unix timestamp at the end to ensure uniqueness and compliance with name constraints.
    """

    # Ensure source object name has at least one alphanumeric character
    source_object_name = (
        re.sub(r"[^a-z0-9]", "", source_object_name.lower()) or "default"
    )

    # Construct the initial part of the job name, ensuring it doesn't start or end with a hyphen
    base_job_name = f"{source_object_name}-{file_date_str}".strip("-")

    # Append file part if it's a valid positive integer
    if str(file_part_str).isdigit() and int(file_part_str) > 0:
        base_job_name += f"-part{file_part_str}"

    # Generate current Unix timestamp in seconds to ensure uniqueness
    current_timestamp = str(int(time.time()))

    # Calculate available length for the base name excluding the timestamp and the separator
    available_length = 63 - len(current_timestamp) - 1

    # Truncate the base job name to fit the available length
    base_job_name = base_job_name[:available_length].rstrip("-")

    # Ensure the truncated base name ends with an alphanumeric character
    while base_job_name and not base_job_name[-1].isalnum():
        base_job_name = base_job_name[:-1]

    # Concatenate the timestamp to ensure uniqueness
    final_job_name = f"{base_job_name}-{current_timestamp}"

    return final_job_name


def get_schema_from_file(schema_file_path: str):
    try:
        schema_file_path_stripped_url = schema_file_path.replace("gs://", "")
        schema_split_url = schema_file_path_stripped_url.split("/", 1)
        bucket_name = schema_split_url[0]
        schema_file_name = schema_split_url[1]
        storage_client = storage.Client()
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(schema_file_name)
        schema = blob.download_as_text()
        schema_data = json.loads(schema)
        schema_columns = [column_dict["name"] for column_dict in schema_data]
    except Exception as e:
        raise Exception(
            f"Problem loading schema for schema_file_path={schema_file_path}, {e} "
        )
    return schema_data, schema_columns
