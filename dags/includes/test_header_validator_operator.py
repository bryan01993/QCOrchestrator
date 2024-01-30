import unittest
from unittest.mock import MagicMock, patch
import pandas as pd

from includes.header_validator_operator import HeaderValidatorOperator


class TestHeaderValidatorOperator(unittest.TestCase):
    def setUp(self):
        # Mocking the necessary components
        self.operator = HeaderValidatorOperator(
            task_id="test_header_validator",
            file_path="dummy_path.csv",
            schema_columns="column1,column2,column3",
            n_header="1",
            delimiter=",",
        )
        self.mock_context = {"ti": MagicMock()}
        self.operator.has_header = True
        self.operator.xcom_push = MagicMock()

    def test_header_matches_schema(self):
        # Mock DataFrame for testing
        mock_df = pd.DataFrame([["column1", "column2", "column3"]])

        # Test _validate_header method
        self.operator._validate_header(
            mock_df, ["column1", "column2", "column3"], self.mock_context
        )

    def test_no_header_but_header_found(self):
        self.operator.has_header = False
        mock_df = pd.DataFrame([["column1", "column2", "column3"]])

        with self.assertRaises(ValueError):
            self.operator._validate_header(
                mock_df, ["column1", "column2", "column3"], self.mock_context
            )

    def test_empty_file(self):
        mock_df = pd.DataFrame([])

        with self.assertRaises(IndexError):
            self.operator._validate_header(
                mock_df, ["column1", "column2", "column3"], self.mock_context
            )

    def test_more_columns_in_file_than_schema(self):
        mock_df = pd.DataFrame([["column1", "column2", "column3", "extra_column"]])

        with self.assertRaises(IndexError):
            self.operator._validate_header(
                mock_df, ["column1", "column2", "column3"], self.mock_context
            )


# Run the tests
if __name__ == "__main__":
    unittest.main()
