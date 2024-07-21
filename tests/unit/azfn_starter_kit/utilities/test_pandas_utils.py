import pandas as pd

from azfn_starter_kit.utilities.pandas_utils import add_column


def test_add_column_with_empty_dataframe():
    df_ = pd.DataFrame({"dummy_column_1": ["dummy_value_1"]})
    expected_df = pd.DataFrame({"dummy_column_1": ["dummy_value_1"], "dummy_column_2": ["dummy_value_2"]})

    actual_df = add_column(df_, "dummy_column_2", "dummy_value_2")

    assert actual_df.equals(expected_df)
