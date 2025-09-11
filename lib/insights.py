import polars as pl


def describe_with_dtypes(df: pl.DataFrame) -> pl.DataFrame:
    """Get describe statistics with data types as the first row."""
    describe_df = df.describe()

    # Convert all numeric columns in describe to string to match dtypes
    describe_df = describe_df.with_columns(
        [pl.col(col).cast(pl.Utf8) for col in describe_df.columns if col != "statistic"]
    )

    # Only include dtypes for columns that are in the describe result
    describe_columns = [col for col in describe_df.columns if col != "statistic"]
    dtypes_dict = {"statistic": "dtype"}

    for col in describe_columns:
        if col in df.columns:
            dtypes_dict[col] = str(df[col].dtype)
        else:
            dtypes_dict[col] = "unknown"

    dtypes_df = pl.DataFrame([dtypes_dict])

    combined_df = pl.concat([dtypes_df, describe_df])
    return combined_df
