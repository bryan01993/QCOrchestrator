import glob
import json
import os

from google.cloud import storage
from jinja2 import Environment, FileSystemLoader
from yaml import load, SafeLoader

from includes.allfunds.entities import (
    ProcessingConfig,
    SourceConfig,
    DestinationConfig,
    FeedConfig,
    FileConfig,
)
from includes.allfunds.exceptions import MissingConfigurationException
from includes.allfunds.loggers import log_warning


def get_local_dag_config_files(config_folder_uri):
    """Return file lists from local folder

    Input : config_folder_uri - String
    Output : Yaml file contents - List (string)
    """

    # check if the local config exists
    if not os.path.exists(config_folder_uri):
        return None

    yaml_configs = []

    # root_dir needs a trailing slash (i.e. /root/dir/)
    for filename in glob.iglob(config_folder_uri + "**/**", recursive=True):
        # Check whether file is in yaml or yml
        if filename.endswith(".yml") or filename.endswith(".yaml"):
            with open(filename, "r") as f:
                yaml_data = load(f.read(), Loader=SafeLoader)
                yaml_configs.append(yaml_data)
        else:
            continue

    return yaml_configs


def get_gcs_dag_config_files(config_folder_uri):
    """Return file lists from GCS

    Input : config_folder_uri - String
    Output : Yaml file contents - List (string)
    """
    client = storage.Client()

    config_folder_uri_parts = config_folder_uri.split("/")
    if "gs://" in config_folder_uri:
        bucket_name = config_folder_uri_parts[2]
        prefix = "/".join(config_folder_uri_parts[3:])
    else:
        bucket_name = config_folder_uri_parts[0]
        prefix = "/".join(config_folder_uri_parts[1:])

    bucket = client.get_bucket(bucket_name)
    dag_config_files = bucket.list_blobs(prefix=prefix)

    yaml_configs = []

    for dag_config_file in dag_config_files:
        if dag_config_file.name.endswith(".yml") or dag_config_file.name.endswith(
            ".yaml"
        ):
            data = dag_config_file.download_as_bytes()
            yaml_data = load(data, Loader=SafeLoader)
            yaml_configs.append(yaml_data)
        else:
            continue

    return yaml_configs


def parse_dag_configuration(yaml_configs):
    """Parse the yaml configuration from multiple files

    Input: dictionary with the configuration (parsed yaml)
    Output: parsed and processed dags config (after applying default values)
    """

    ingestion_dags = []

    def map_yaml_config_to_conf_object(yaml_config, defaults):
        processing_config = ProcessingConfig()
        source_config = SourceConfig()
        destination_config = DestinationConfig()
        feed_config = FeedConfig()
        file_config = FileConfig()

        # define default values based on the optional defaults configuration
        default_dag_prefix = defaults.get("dag_config", {}).get(
            "dag_prefix", "common_layer"
        )
        default_schedule = yaml_config.get("destination_config", {}).get(
            "dag_frequency", None
        )
        default_files_per_day = yaml_config.get("destination_config", {}).get(
            "files_per_day", 1
        )
        default_owner = defaults.get("dag_config", {}).get(
            "owner", None
        )  # Would be Owner/Responsible TAG?
        default_start_date = defaults.get("dag_config", {}).get("start_date", None)

        default_processing_bucket = defaults.get("processing_config", {}).get(
            "processing_bucket", None
        )
        default_dataproc_staging_bucket = defaults.get("processing_config", {}).get(
            "dataproc_staging_bucket", None
        )

        default_landing_bucket = defaults.get("source_config", {}).get(
            "landing_bucket", None
        )
        default_staging_bucket = defaults.get("source_config", {}).get(
            "staging_bucket", None
        )
        default_dataproc_bucket = defaults.get("source_config", {}).get(
            "dataproc_bucket", None
        )
        default_schemas_bucket = defaults.get("source_config", {}).get(
            "schemas_bucket", None
        )

        default_target_project_id = defaults.get("destination_config", {}).get(
            "target_project_id", None
        )

        default_ingest_mode = yaml_config.get("destination_config", {}).get(
            "ingest_mode", None
        )
        default_delimiter = yaml_config.get("file_config", {}).get("delimiter", None)
        default_n_header = yaml_config.get("file_config", {}).get("n_header", None)
        default_n_footer = yaml_config.get("file_config", {}).get("n_footer", None)

        # set the values
        # name (not to be used, apart from logging / exception handling)
        name = yaml_config.get("name", None)

        default_source_format = yaml_config.get("source_config", {}).get(
            "source_format", None
        )
        default_responsible = yaml_config.get("source_config", {}).get(
            "responsible", None
        )
        default_source = yaml_config.get("source_config", {}).get("source", None)
        default_schema_file_name = yaml_config.get("source_cofing", {}).get(
            "source_schema_file_name", None
        )

        # Dag Config

        # dag_id
        dag_prefix = yaml_config.get("dag_config", {}).get(
            "dag_prefix", default_dag_prefix
        )
        if dag_prefix is None:
            raise MissingConfigurationException(name=name, field="dag_prefix")
        feed_config.dag_id = f"{dag_prefix}_{name}"

        # schedule
        schedule = yaml_config.get("destination_config", {}).get(
            "dag_frequency", default_schedule
        )
        if schedule is None:
            raise MissingConfigurationException(name=name, field="schedule")
        feed_config.schedule = schedule

        # files_per_day
        files_per_day = yaml_config.get("destination_config", {}).get(
            "files_per_day", default_files_per_day
        )
        if files_per_day is None:
            raise MissingConfigurationException(name=name, field="files_per_day")
        feed_config.files_per_day = files_per_day

        # default_args
        owner = yaml_config.get("dag_config", {}).get("owner", default_owner)
        start_date = yaml_config.get("dag_config", {}).get(
            "start_date", default_start_date
        )

        if owner is None:
            raise MissingConfigurationException(name=name, field="owner")

        if start_date is None:
            raise MissingConfigurationException(name=name, field="start_date")

        default_args = {"owner": owner, "start_date": start_date}

        feed_config.default_args = default_args

        # Processing Config

        # staging bucket
        source_config.staging_bucket = yaml_config.get("source_config", {}).get(
            "staging_bucket", default_staging_bucket
        )
        if source_config.staging_bucket is None:
            raise MissingConfigurationException(name=name, field="staging_bucket")

        # processing bucket
        processing_config.processing_bucket = yaml_config.get(
            "processing_config", {}
        ).get("processing_bucket", default_processing_bucket)
        if processing_config.processing_bucket is None:
            raise MissingConfigurationException(name=name, field="processing_bucket")

        # dataproc_staging bucket
        processing_config.dataproc_staging_bucket = yaml_config.get(
            "processing_config", {}
        ).get("dataproc_staging_bucket", default_dataproc_staging_bucket)
        if processing_config.dataproc_staging_bucket is None:
            raise MissingConfigurationException(
                name=name, field="dataproc_staging_bucket"
            )

        # Source Config

        # landing_bucket
        source_config.landing_bucket = yaml_config.get("source_config", {}).get(
            "landing_bucket", default_landing_bucket
        )
        if source_config.landing_bucket is None:
            raise MissingConfigurationException(name=name, field="landing_bucket")

        # source
        source_config.source = yaml_config.get("source_config", {}).get(
            "source", default_source
        )
        if source_config.landing_bucket is None:
            raise MissingConfigurationException(name=name, field="source")

        # responsible
        source_config.responsible = yaml_config.get("source_config", {}).get(
            "responsible", default_responsible
        )
        if source_config.landing_bucket is None:
            raise MissingConfigurationException(name=name, field="responsible")

        # dataproc_bucket
        source_config.dataproc_bucket = yaml_config.get("source_config", {}).get(
            "dataproc_bucket", default_dataproc_bucket
        )
        if source_config.dataproc_bucket is None:
            raise MissingConfigurationException(name=name, field="dataproc_bucket")

        # schemas_bucket
        source_config.schemas_bucket = yaml_config.get("source_config", {}).get(
            "schemas_bucket", default_schemas_bucket
        )
        if source_config.schemas_bucket is None:
            raise MissingConfigurationException(name=name, field="schemas_bucket")

        # source_objects_prefix
        source_config.source_objects_prefix = yaml_config.get("source_config", {}).get(
            "source_objects_prefix", None
        )
        if source_config.source_objects_prefix is None:
            raise MissingConfigurationException(
                name=name, field="source_objects_prefix"
            )

        # source_format
        source_config.source_format = yaml_config.get("source_config", {}).get(
            "source_format", default_source_format
        )
        if source_config.source_format is None:
            raise MissingConfigurationException(name=name, field="source_format")
            # figurationException(name=name, field="schema_file")

        # source_schema_file_name
        source_config.source_schema_file_name = yaml_config.get(
            "source_config", {}
        ).get("source_schema_file_name", default_schema_file_name)
        if source_config.source_schema_file_name is None:
            raise MissingConfigurationException(
                name=name, field="source_schema_file_name"
            )

        # Destination Config

        # target_project_id
        destination_config.target_project_id = yaml_config.get(
            "destination_config", {}
        ).get("target_project_id", default_target_project_id)
        if destination_config.target_project_id is None:
            raise MissingConfigurationException(name=name, field="target_project_id")

        # # processed bucket
        # destination_config.store_bucket = yaml_config.get("destination_config", {}).get("store_bucket", default_store_bucket)
        # if destination_config.store_bucket is None:
        #     raise MissingConfigurationException(name=name, field="store_bucket")

        # dataset_name
        destination_config.dataset_name = yaml_config.get("destination_config", {}).get(
            "dataset_name", None
        )
        if destination_config.dataset_name is None:
            raise MissingConfigurationException(name=name, field="dataset_name")

        # table_name
        destination_config.table_name = yaml_config.get("destination_config", {}).get(
            "table_name", None
        )
        if destination_config.table_name is None:
            raise MissingConfigurationException(name=name, field="table_name")

        # ingest_mode
        destination_config.ingest_mode = yaml_config.get(
            "destination_config", {}
        ).get("ingest_mode", default_ingest_mode)
        # if destination_config.write_disposition is None:
        #     raise MissingConfigurationException(name=name, field="write_disposition")

        # delimiter
        file_config.delimiter = yaml_config.get("file_config", {}).get(
            "delimiter", default_delimiter
        )
        # columns_format
        file_config.columns_format = yaml_config.get("file_config", {}).get(
            "columns_format", None
        )

        # fixed_widths
        file_config.fixed_widths = yaml_config.get("file_config", {}).get("fixed_widths", None)


        if file_config.delimiter is None:
            raise MissingConfigurationException(name=name, field="delimiter")

        # header
        file_config.n_header = yaml_config.get("file_config", {}).get(
            "n_header", default_n_header
        )
        if file_config.n_header is None:
            raise MissingConfigurationException(name=name, field="n_header")

        # footer
        file_config.n_footer = yaml_config.get("file_config", {}).get(
            "n_footer", default_n_footer
        )
        if file_config.n_footer is None:
            raise MissingConfigurationException(name=name, field="n_footer")

        feed_config.processing_config = processing_config
        feed_config.source_config = source_config
        feed_config.destination_config = destination_config
        feed_config.file_config = file_config

        return feed_config

    # first process raw yaml files (no defaults are known yet)
    unprocessed_yaml_configs = []
    yaml_config_defaults = {}
    for single_yaml_config in yaml_configs:
        if "feeds" in single_yaml_config:
            if isinstance(single_yaml_config["feeds"], list):
                for feed_config in single_yaml_config["feeds"]:
                    unprocessed_yaml_configs.append(feed_config)
            elif isinstance(single_yaml_config["feeds"], dict):
                unprocessed_yaml_configs.append(single_yaml_config["feeds"])
        elif "defaults" in single_yaml_config:
            yaml_config_defaults = single_yaml_config["defaults"]

    # second process the yamls files with defaults known (if exception has occured, ignore the config element)
    try:
        for yaml_config in unprocessed_yaml_configs:
            ingestion_dags.append(
                map_yaml_config_to_conf_object(yaml_config, yaml_config_defaults)
            )
    except MissingConfigurationException as ex:
        log_warning(msg=f"{type(ex).__name__} was raised: {ex}")

    return ingestion_dags


def prepare_batch_payload(**kwargs):
    """Prepare the batch payload to be used in Dataproc to load files to Cloud SQL

    Args:
    input_bucket - input bucket to search for ingestion files
    source_format - a file format to choose from
    table_name - a destination table

    Returns: dictionary containing the batch payload for the Dataproc job
    """

    env = Environment(
        loader=FileSystemLoader(os.path.join(os.path.dirname(__file__), "templates")),
        autoescape=True,
    )
    template = env.get_template("batch_payload.json")

    return json.loads(template.render(**kwargs))


def removesuffix(string, suffix):
    if string.endswith(suffix):
        return string[: -len(suffix)]
    return string
