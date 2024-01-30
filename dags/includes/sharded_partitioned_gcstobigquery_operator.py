from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator

from airflow.providers.google.cloud.transfers.gcs_to_bigquery import (
    GCSToBigQueryOperator,
)
from airflow.utils.decorators import apply_defaults


class ShardedPartitionedGCSToBigQueryOperator(GCSToBigQueryOperator):
    """
    ShardedPartitionedGCSToBigQueryOperator is a custom operator that extends the GCSToBigQueryOperator
    to handle sharding and partitioning.

    This operator adds 'file_part' parameter that, when greater than 0, modifies the destination
    table name to include the 'file_part' as a suffix and creates a unified BigQuery view that aggregates
    all the sharded tables.

    :param file_part: Suffix to be added to the destination table name if greater than 0.
    :type file_part: int, templated
    """

    @apply_defaults
    def __init__(self, file_part=0, file_uncompressed_path="", *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.file_part = file_part
        self.file_uncompressed_path = file_uncompressed_path
        self.template_fields = tuple(
            list(self.template_fields) + ["file_part", "file_uncompressed_path"]
        )

    def execute(self, context):
        """
        Overriding the execute method to include custom behavior for sharding and partitioning.
        :param context: The execution context.
        :type context: dict
        """
        if "part" not in self.file_uncompressed_path:
            self.file_part = 0
        try:
            self.file_part = int(self.file_part)
        except ValueError as e:
            self.log.error(f"Failed to convert file_part to integer: {e}")
            raise e

        original_table: str = self.destination_project_dataset_table

        try:
            if self.file_part > 0:
                if "$" in original_table:
                    table_base, table_partition = original_table.split("$", 1)
                    self.destination_project_dataset_table = (
                        f"{table_base}_{self.file_part}${table_partition}"
                    )
                else:
                    self.destination_project_dataset_table = (
                        f"{original_table}_{self.file_part}"
                    )
        except Exception as e:
            self.log.error(f"Failed to update destination table name: {e}")
            raise e

        try:
            super().execute(context)
        except Exception as e:
            self.log.error(f"Failed to execute parent class method: {e}")
            raise e

        if self.file_part > 0:
            original_table = original_table.replace(":", ".", 1)
            table_base, _ = original_table.split("$", 1)
            description = f"Unified view for all shards of {table_base}"
            _create_view_sql = (
                f"CREATE VIEW IF NOT EXISTS {table_base} OPTIONS("
                f"description='{description}')"
                f" AS SELECT *, _TABLE_SUFFIX as FILE_PART FROM `{table_base}_*`"
            )

            configuration = {
                "query": {"query": _create_view_sql, "useLegacySql": False}
            }

            try:
                create_view = BigQueryInsertJobOperator(
                    task_id="create_unified_view", configuration=configuration
                )
                create_view.execute(context)
            except Exception as e:
                self.log.error(f"Failed to create unified view: {e}")
                raise e
