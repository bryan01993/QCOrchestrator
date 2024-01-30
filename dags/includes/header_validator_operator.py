import json
from typing import Tuple, List

import pandas as pd
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from google.cloud import storage


class HeaderValidatorOperator(BaseOperator):
    VALID_MATCHES_PERCENT = 20
    template_fields = ("file_path", "schema_columns", "n_header", "delimiter")

    @apply_defaults
    def __init__(
        self,
        file_path: str,
        schema_columns: str,
        n_header: str,
        delimiter: str,
        *args,
        **kwargs,
    ):
        super(HeaderValidatorOperator, self).__init__(*args, **kwargs)
        self.file_path = file_path
        self.schema_columns = schema_columns
        self.n_header = n_header
        self.delimiter = delimiter

    def execute(self, context):
        import ast

        self.n_header = int(self.n_header)
        self.n_header = 0 if self.n_header == 0 else self.n_header - 1
        self.skip_lines = 0 if self.n_header <= 1 else self.n_header - 1
        self.has_header = True if self.n_header > 0 else False
        data = self._read_files(sep=self.delimiter)

        if isinstance(self.schema_columns, str):
            try:
                self.schema_columns = ast.literal_eval(self.schema_columns)
            except (ValueError, SyntaxError):
                raise ValueError(
                    "schema_columns debe ser una lista o una representación de lista válida."
                )

        self._validate_header(data, self.schema_columns, context)

    def _read_files(self, sep) -> pd.DataFrame:
        """
        Reads data from the specified input file and schema from the specified schema file.
        For the data file, if a header is expected (has_header = True), the method reads only the
        number of rows specified by n_header. If no header is expected, only the first row is read.
        The method is primarily used to fetch data information to be used by the
        _validate_header method.

        Returns:
         pd.DataFrame: a df containing data read from the input file.
        """
        self.log.info("Staring reading files func")
        self.log.info(f"Reading files with separator '{sep}'")
        try:
            if self.has_header:
                self.log.info(
                    f"{self.file_path} || {self.n_header} || {self.skip_lines} || {sep}"
                )
                data = pd.read_csv(
                    self.file_path,
                    nrows=self.n_header - self.skip_lines,
                    skiprows=self.skip_lines,
                    header=None,
                    encoding="latin1",
                    sep=sep,
                )
            else:
                data = pd.read_csv(
                    self.file_path, nrows=1, header=None, encoding="latin1", sep=sep
                )
        except pd.errors.ParserError as e:
            self.log.error(f"Error parsing CSV file: {e}")
            raise
        except Exception as e:
            raise Exception(
                f"Unexpected error error loading the file {self.file_path}"
            ) from e
        return data

    def _validate_header(
        self, data: pd.DataFrame, schema_columns: list, context: dict
    ) -> None:
        """
        Validates the header of a data file against a provided schema.

        This method checks if the headers in the data file match the expected headers in the schema.
         It raises an exception if:
        - Headers are supposed to exist but do not match with the schema.
        - Headers are not supposed to exist but a possible header is found.

        Parameters:
        - data (pd.DataFrame): DataFrame containing data read from the input file.
        - schema_columns (List[str]): List of expected column names from the schema.
        - context (dict): Airflow context dictionary containing execution-related information.

        Raises:
        - IndexError: If the file appears empty or has more columns than expected in the schema.
        - ValueError: If a possible header is found when none is expected according to the configuration.

        Returns:
        - None
        """
        n_header_row = list(data.iloc[-1])

        # handling empty files
        if len(n_header_row) == 0 and data.empty:
            self.log.error(f"n_header_row: {n_header_row}")
            self.log.error(f"len(n_header_row): {len(n_header_row)}")
            raise IndexError("The file looks like empy")

        # handling last column is empy because the last character of the row is a delimiter.
        if len(n_header_row) - 1 == len(schema_columns):
            self.log.warning(
                "Last column will be poped from the list, assuming its the last separator"
            )
            n_header_row.pop()

        # Prevent files with more columns than the schema.
        if len(n_header_row) > len(schema_columns):
            self.log.error(f"n_header_row: {n_header_row}")
            self.log.error(f"len(n_header_row): {len(n_header_row)}")
            self.log.error(f"len(schema_columns): {len(schema_columns)}")
            raise IndexError(
                "The file contains more columns that the schema, please update the schema and try again"
            )

        schema_and_schema_row_matches = [
            self.__normalize_column_name(schema_column)
            == self.__normalize_column_name(header_column)
            for schema_column, header_column in zip(schema_columns, n_header_row)
        ]

        schema_and_schema_row_match_percent = (
            sum(schema_and_schema_row_matches) / len(schema_columns) * 100
        )

        mismatch_dict = {
            schema_column: header_column
            for schema_column, header_column in zip(schema_columns, n_header_row)
            if self.__normalize_column_name(schema_column)
            != self.__normalize_column_name(header_column)
        }

        self.log.warning("Staring common_logs")

        self.log.info(f"schema_columns: {schema_columns}")
        self.log.info(f"n_header_row: {n_header_row}")
        self.log.info(f"match_percent: {schema_and_schema_row_match_percent}")
        self.log.warning(
            f"Unmatched columns list comparing schema_columns vs n_header_row: "
            f"{mismatch_dict if mismatch_dict else 'NO Unmatched columns'}"
        )

        self.log.warning("Ending common_logs")

        if self.has_header:
            if schema_and_schema_row_match_percent >= self.VALID_MATCHES_PERCENT:
                self.log.info(
                    f"Header and schema match more than {self.VALID_MATCHES_PERCENT}%"
                    f", it will be removed in next step"
                )
            else:
                self.log.warning(
                    f"Config set n_header={self.n_header} but too low match_percent: "
                    f"{schema_and_schema_row_match_percent}% -> n_header_row: {set(n_header_row)}"
                )
                self.log.warning("Setting XCOM n_header to 0")
                self.xcom_push(context, key="n_header", value=0)
        else:
            if schema_and_schema_row_match_percent >= self.VALID_MATCHES_PERCENT:
                warning_msg = (
                    f"Config says no header -n_header={self.n_header}- but possible header found with match_percent: "
                    f"{schema_and_schema_row_match_percent}% -> n_header_row: {set(n_header_row)}"
                    f"Manual validation is required for continue."
                )
                self.log.warning(warning_msg)
                raise ValueError(warning_msg)
            else:
                self.log.info("Config says no header and none was found.")

    @staticmethod
    def __normalize_column_name(name: str) -> str:
        return str(name).upper().strip().replace(" ", "_")
