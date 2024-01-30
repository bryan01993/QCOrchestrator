import os
from configparser import ConfigParser

config = ConfigParser()

config.read(os.path.join(os.path.dirname(__file__), "config.ini"))

class GCP:
    PROJECT_ID = os.getenv("PROJECT_ID")

    class DAG:
        class CONFIG:
            FOLDER_PATH_GCS = "gs://" + os.getenv("GCS_CONFIG_PATH")
            FOLDER_PATH_LOCAL = config["dags"]["default_config_path"]


DABCONS_TABLES = [
    'BnpPositions_raw_DABCONS18000', 'BnpPositions_raw_DABCONS18001',
    'BnpPositions_raw_DABCONS18000_posplit',
    'BnpMovements_raw_DABCONS18000', 'BnpMovements_raw_DABCONS18001'
]
allowed_extensions = [".dat", ".txt", ".csv"]
allowed_extensions_zip = [".zip"]
allowed_extensions_read = [".dat", ".txt" ".csv", ".xlsx"]
