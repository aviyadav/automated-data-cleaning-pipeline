import pandas as pd
from utils import time_it

@time_it
def main():
    print("Hello from automated-data-cleaning-pipeline!")

@time_it
def handling_missing_values(df: pd.DataFrame, method="mean", fill_value=None) -> pd.DataFrame:
    if method == 'drop':
        return df.dropna()
    elif method == 'fill':
        return df.fillna(fill_value)
    elif method == 'mean':
        return df.fillna(df.mean(numeric_only=True))
    else:
        raise ValueError("Invalid method. Choose from 'drop', 'fill', or 'mean'.")
    
    return df

@time_it
def remove_duplicates(df: pd.DataFrame, subset=None) -> pd.DataFrame:
    return df.drop_duplicates(subset=subset)


@time_it
def transform_data_types(df: pd.DataFrame, col_types: dict) -> pd.DataFrame:
    for col, dtype in col_types.items():
        # Round float values before converting to integer types
        if dtype in ['int64', 'Int64', 'int32', 'Int32', 'int16', 'Int16', 'int8', 'Int8']:
            df[col] = df[col].round().astype(dtype)
        else:
            df[col] = df[col].astype(dtype)
    return df


@time_it
def data_cleaning_pipeline(df: pd.DataFrame, missing_values_method='mean', fill_value=None, subset=None, col_types=None) -> pd.DataFrame:
    df = handling_missing_values(df, method=missing_values_method, fill_value=fill_value)
    df = remove_duplicates(df, subset=subset)
    if col_types:
        df = transform_data_types(df, col_types)
    return df


if __name__ == "__main__":
    # main()
    data = {
        'Name': ['Alice', 'Bob', None, 'David'],
        'Age': [25, None, 30, 22],
        'Salary': [50000.0, 60000.0, None, 55000.0],
        'Department': ['HR', 'Finance', 'IT', None],
        'Joining_Date': ['2020-01-15', None, '2019-03-22', '2021-07-30'],
        'Is_Active': [True, None, False, True],
        'Performance_Score': [85.5, 90.0, None, 88.0]}

    df =pd.DataFrame(data)
    print(f"Original DataFrame:\n{df}")

    # cleaned_df = handling_missing_values(df, method="mean")
    # print(f"Cleaned DataFrame:\n{cleaned_df}")

    # deduped_df = remove_duplicates(cleaned_df)
    # print(f"Deduplicated DataFrame:\n{deduped_df}")

    col_types = {'Age': 'Int64', 'Joining_Date': 'datetime64[ns]', 'Is_Active': 'boolean', 'Performance_Score': 'float', 'Salary': 'float'}

    # transformed_df = transform_data_types(df, col_types)
    # print(f"Transformed DataFrame:\n{transformed_df}")

    pipeline_df = data_cleaning_pipeline(df, missing_values_method='mean', subset=['Name', 'Age'], col_types=col_types)
    print(f"DataFrame after Data Cleaning Pipeline:\n{pipeline_df}")