import polars as pl


def df_clean_all(df: pl.DataFrame) -> pl.DataFrame:
    # Clean column names
    df = df_clean_columns(df)

    # Clean row/column data
    df = df_clean_contents(df)

    # Redetermine types now df is clean
    df = redetermine_types(df)

    # Return final result
    return df


SAFE_COLUMN_SPACE_CHAR: str = "_"
UNSAFE_COLUMN_SPACE_CHARS: list[str] = [" ", "-"]
UNSAFE_COLUMN_GENERAL_CHARS: list[str] = [
    "!",
    "@",
    "#",
    "$",
    "%",
    "^",
    "&",
    "*",
    "(",
    ")",
    "+",
    "=",
    "{",
    "}",
    "[",
    "]",
    ":",
    ";",
    '"',
    "'",
    "<",
    ">",
    ",",
    ".",
    "?",
    "/",
    "|",
    "\\",
]


def df_clean_columns(df: pl.DataFrame) -> pl.DataFrame:
    print("Cleaning Columns")
    cols_clean = []
    for col_name in df.columns:
        # strip whitespace outer ends
        sanitized = col_name.strip()

        # replace inner spaces with chosen separator
        for char in UNSAFE_COLUMN_SPACE_CHARS:
            sanitized = sanitized.replace(char, SAFE_COLUMN_SPACE_CHAR)

        # purge unsupported chars
        for char in UNSAFE_COLUMN_GENERAL_CHARS:
            sanitized = sanitized.replace(char, "")

        cols_clean.append(sanitized)

    return df.rename(dict(zip(df.columns, cols_clean)))


NULL_VALUE_STRINGS = ["*", "N/A", "N.A.", "#N/A", "???", "NULL", "null"]


def df_clean_contents(df: pl.DataFrame) -> pl.DataFrame:
    print("Cleaning null-ish values")
    string_columns = [col for col in df.columns if df[col].dtype == pl.Utf8]
    if string_columns:
        df = df.with_columns(
            [
                pl.col(col)
                .str.strip_chars()  # Remove leading/trailing whitespace
                .map_elements(
                    lambda x: None if x and x in NULL_VALUE_STRINGS else x,
                    return_dtype=pl.Utf8,
                )
                for col in string_columns
            ]
        )

    print("Dropping rows with all-null values")
    df = df.filter(~pl.all_horizontal(pl.all().is_null()))

    print("Dropping columns with all-null values")
    df = df.select([col for col in df.columns if not df[col].is_null().all()])

    return df


def redetermine_types(df: pl.DataFrame) -> pl.DataFrame:
    """Intelligently redetermine better data types for columns in dataframe"""
    regex_parentheses_negative = r"^\s*\(([0-9,.]+)\)\s*$"
    regex_currency_cleanup = r"[$¢£¥€₹₽¤,\s]"

    print("Converting plain numeric strings to floats")
    df = df.with_columns(
        [
            (
                pl.col(col).cast(pl.Float64, strict=False)
                if (
                    df[col].dtype == pl.Utf8
                    and df[col].cast(pl.Float64, strict=False).is_not_null().all()
                )
                else pl.col(col)
            )
            for col in df.columns
        ]
    )

    print("Converting paranthesis-wrapped negative values to floats")
    df = df.with_columns(
        [
            (
                pl.col(col)
                .str.replace_all(regex_parentheses_negative, r"-$1")
                .str.replace_all(regex_currency_cleanup, "")
                .cast(pl.Float64)
                if (
                    df[col].dtype == pl.Utf8
                    and df[col].str.contains(regex_parentheses_negative).any()
                )
                else pl.col(col)
            )
            for col in df.columns
        ]
    )

    print("Converting currency-formatted values to floats")
    df = df.with_columns(
        [
            (
                pl.col(col).str.replace_all(regex_currency_cleanup, "").cast(pl.Float64)
                if (
                    df[col].dtype == pl.Utf8
                    and df[col]
                    .str.replace_all(regex_currency_cleanup, "")
                    .cast(pl.Float64, strict=False)
                    .is_not_null()
                    .all()
                )
                else pl.col(col)
            )
            for col in df.columns
        ]
    )

    print("Converting whole-number floats to integers")
    df = df.with_columns(
        [
            (
                pl.col(col).cast(pl.Int64)
                if (df[col].dtype == pl.Float64 and (df[col] == df[col].floor()).all())
                else pl.col(col)
            )
            for col in df.columns
        ]
    )

    print("Converting date strings to date types")
    df = df.with_columns(
        [
            (
                pl.col(col).cast(pl.Utf8).str.to_date("%Y-%m-%d", strict=False)
                if (
                    df[col].dtype in [pl.Utf8, pl.Categorical]
                    and df[col]
                    .cast(pl.Utf8)
                    .str.to_date("%Y-%m-%d", strict=False)
                    .is_not_null()
                    .all()
                )
                else pl.col(col)
            )
            for col in df.columns
        ]
    )

    print("Converting low-cardinality strings to categoricals")
    df = df.with_columns(
        [
            (
                pl.col(col).cast(pl.Categorical)
                if (
                    df[col].dtype == pl.Utf8
                    and df[col].n_unique() <= min(250, df.height * 0.10)
                )
                else pl.col(col)
            )
            for col in df.columns
        ]
    )

    print("Shrinking integer and float dtypes for optimal compression")
    df = df.with_columns(
        [
            df[col].shrink_dtype().alias(col)
            for col in df.columns
            if df[col].dtype
            in [pl.Int64, pl.Int32, pl.Int16, pl.Int8, pl.Float64, pl.Float32]
        ]
    )

    print("Shrinking memory allocation")
    df = df.shrink_to_fit()

    return df
