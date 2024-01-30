from airflow.providers.google.cloud.operators.dataform import (
    DataformCreateCompilationResultOperator,
)


class TemplatedDataformCreateCompilationResultOperator(
    DataformCreateCompilationResultOperator
):
    """
    Extends DataformCreateCompilationResultOperator to allow templated compilation results.

    This custom operator allows for dynamic assignment of the `compilation_result` field
    by making it a templated field. This enables the use of Jinja templates to populate
    this field at runtime.
    """

    template_fields = DataformCreateCompilationResultOperator.template_fields + (
        "compilation_result",
    )

    def execute(self, context):
        self.log.info(
            f"Executing CustomDataformCreateCompilationResultOperator with compilation_result: {self.compilation_result}"
        )

        result = super().execute(context)

        self.log.info(f"Compilation result as dictionary: {result}")
        return result
