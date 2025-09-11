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
    # Sanitise null-ish values inside cell rows
    # TODO: Handle NA repetition better
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

    # Drop rows where ALL columns are null (completely empty rows)
    df = df.filter(~pl.all_horizontal(pl.all().is_null()))

    # Drop columns where ALL values are null (completely empty columns)
    df = df.select([col for col in df.columns if not df[col].is_null().all()])

    return df


def redetermine_types(df: pl.DataFrame) -> pl.DataFrame:
    """Intelligently redetermine better data types for columns in dataframe"""
    money_pattern_match: str = r"^-?\d*\.?\d+$"
    money_pattern_strip: str = r"[()$]"
    return (
        df.with_columns(
            [  # Cast to number if all columns can safely convert
                pl.col(col).cast(pl.Decimal)
                if df[col].dtype == pl.Utf8
                and df[col]
                .str.replace_all(money_pattern_strip, "")
                .cast(pl.Decimal, strict=False)
                .is_not_null()
                .all()
                else pl.col(col)
                for col in df.columns
            ]
        )
        .with_columns(
            [  # Cast to number if the field is a money pattern and can convert if chars are removed
                pl.when(pl.col(col).str.contains(money_pattern_match))
                .then(
                    pl.col(col)
                    .str.replace_all(money_pattern_strip, "")
                    .cast(pl.Decimal)
                )
                .otherwise(pl.col(col).cast(pl.Decimal))
                if df[col].dtype == pl.Utf8
                and df[col]
                .str.replace_all(money_pattern_strip, "")
                .cast(pl.Decimal, strict=False)
                .is_not_null()
                .all()
                else pl.col(col)
                for col in df.columns
            ]
        )
        .with_columns(
            [  # Cast to Categorical for string columns with low cardinality
                pl.col(col).cast(pl.Categorical)
                if df[col].dtype == pl.Utf8 and df[col].n_unique() / df.height < 0.5
                else pl.col(col)
                for col in df.columns
            ]
        )
    )
