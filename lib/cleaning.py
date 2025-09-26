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
    regex_parentheses_negative = r"^\s*\(([0-9,.]+)\)\s*$"
    regex_currency_cleanup = r"[$¢£¥€₹₽¤,\s]"
    
    # Convert parentheses-wrapped negative values to Decimal: "(123.45)" -> -123.45
    df = df.with_columns([
        pl.col(col)
        .str.replace_all(regex_parentheses_negative, r"-$1")  # (123) -> -123
        .str.replace_all(regex_currency_cleanup, "")  # Remove currency, commas, spaces
        .cast(pl.Decimal)
        if (df[col].dtype == pl.Utf8 and 
            df[col].str.contains(regex_parentheses_negative).any())
        else pl.col(col)
        for col in df.columns
    ])
    
    # Convert currency-formatted strings to Decimal: "$1,234.56" -> 1234.56
    df = df.with_columns([
        pl.col(col)
        .str.replace_all(regex_currency_cleanup, "")
        .cast(pl.Decimal)
        if (df[col].dtype == pl.Utf8 and 
            df[col].str.replace_all(regex_currency_cleanup, "")
                   .cast(pl.Decimal, strict=False)
                   .is_not_null()
                   .all())
        else pl.col(col)
        for col in df.columns
    ])
    
    # Convert low-cardinality string columns to Categorical for memory efficiency
    df = df.with_columns([
        pl.col(col).cast(pl.Categorical)
        if (col in df.columns and 
            df[col].dtype == pl.Utf8 and 
            df[col].n_unique() / df.height < 0.5)
        else pl.col(col)
        for col in df.columns
    ])
    
    return df
