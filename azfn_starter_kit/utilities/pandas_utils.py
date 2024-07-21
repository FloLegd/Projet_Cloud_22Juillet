import pandas as pd


def add_column(df_: pd.DataFrame, column_name: str, value: str = "dummy_value") -> pd.DataFrame:
    df_[column_name] = value
    return df_
