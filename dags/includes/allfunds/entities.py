
from dataclasses import dataclass, field, asdict
from typing import Dict
import json

@dataclass
class ProcessingConfig:
    """Data class containing processing configuration"""
    processing_bucket: str = ""
    dataproc_staging_bucket: str = ""

    def __str__(self):
        return json.dumps(asdict(self), indent=4, sort_keys=False, default=str)

@dataclass
class SourceConfig:
    """Data class containing data source configuration"""
    landing_bucket: str = ""
    staging_bucket: str = ""
    source_format: str = ""
    source_objects_prefix: str = ""
    source_schema_file_name: str = ""
    responsible: bool = True
    source: str = ""
    def __str__(self):
        return json.dumps(asdict(self), indent=4, sort_keys=False, default=str)

@dataclass
class DestinationConfig:
    """Data class containing data destination configuration"""

    target_project_id: str = ""
    dataset_name: str = ""
    table_name: str = ""
    ingest_mode: str = ""
    delimiter: str = ""
    active: bool = True
    dag_frequency: str = None
    files_per_day: int = 1

    def __str__(self):
        return json.dumps(asdict(self), indent=4, sort_keys=False, default=str)

@dataclass
class FileConfig:
    """Data class containing file configuration"""

    delimiter: str = ""
    n_header: int = 0
    n_footer: int = 0
    last_semicolon: str = ""
    split: bool = False

    def __str__(self):
        return json.dumps(asdict(self), indent=4, sort_keys=False, default=str)

@dataclass
class FeedConfig:
    """Data class containing dag configuration to be used by the dynamic dag generator"""
    dag_id: str = ""
    schedule: str = ""
    default_args: dict = field(default_factory=dict)
    processing_config: ProcessingConfig = None
    source_config: SourceConfig = None
    destination_config: DestinationConfig = None
    file_config: FileConfig = None

    def __str__(self):
        return json.dumps(asdict(self), indent=4, sort_keys=False, default=str)
