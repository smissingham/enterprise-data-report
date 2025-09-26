import polars as pl
from itertools import combinations

def find_ranked_composite_keys(
    df: pl.DataFrame,
    n_candidates: int = 5,
    max_key_size: int = 4,
    sample_size: int = 50000,
    prefer_patterns: list = None,
):
    """Find top N keys with intelligent ranking, optimized for performance"""

    columns = df.columns
    total_rows = df.height
    candidates = []
    prefer_patterns = prefer_patterns or [
        "id",
        "key",
        "number",
        "code",
        "invoice",
        "document",
    ]

    # Filter out decimal columns but keep integers
    eligible_columns = _filter_decimal_columns(df, columns)

    if not eligible_columns:
        return []

    sample_df = df.sample(min(sample_size, total_rows))

    # Check single columns first - return immediately if found
    single_column_candidates = []
    for col in eligible_columns:
        unique_count = sample_df.select([col]).unique().height
        if unique_count == sample_df.height:
            full_unique_count = df.select([col]).unique().height
            if full_unique_count == total_rows:
                score = calculate_preference_score([col], prefer_patterns)
                single_column_candidates.append(([col], 1, score))

    # If we found single-column keys, return the best one immediately
    if single_column_candidates:
        single_column_candidates.sort(key=lambda x: -x[2])
        return [single_column_candidates[0][0]]

    # Only check multi-column combinations if no single column works
    for size in range(2, min(max_key_size + 1, len(eligible_columns) + 1)):
        size_candidates = []

        for col_combo in combinations(eligible_columns, size):
            unique_count = sample_df.select(col_combo).unique().height
            if unique_count == sample_df.height:
                full_unique_count = df.select(col_combo).unique().height
                if full_unique_count == total_rows:
                    score = calculate_preference_score(col_combo, prefer_patterns)
                    size_candidates.append((list(col_combo), size, score))

        if size_candidates:
            # Sort by preference score descending for this size
            size_candidates.sort(key=lambda x: -x[2])
            candidates.extend(size_candidates)

            # For minimum set: if we found keys of this size, we're done
            if len(candidates) >= n_candidates:
                break

    return [combo for combo, size, score in candidates[:n_candidates]]


def _filter_decimal_columns(df: pl.DataFrame, columns: list) -> list:
    """Filter out columns containing decimal values, keep integers and non-numeric"""
    eligible = []

    for col in columns:
        dtype = df[col].dtype

        # Keep non-numeric columns
        if not dtype.is_numeric():
            eligible.append(col)
            continue

        # For numeric columns, check if they contain decimals
        if dtype in [pl.Float32, pl.Float64]:
            # Sample to check if values are actually integers
            sample_values = df[col].drop_nulls().head(1000).to_list()
            if sample_values and all(
                float(val).is_integer() for val in sample_values if val is not None
            ):
                eligible.append(col)
        elif dtype in [
            pl.Int8,
            pl.Int16,
            pl.Int32,
            pl.Int64,
            pl.UInt8,
            pl.UInt16,
            pl.UInt32,
            pl.UInt64,
        ]:
            # Integer types are always eligible
            eligible.append(col)
        # Skip other numeric types (decimals)

    return eligible


def calculate_preference_score(col_combo, prefer_patterns):
    """Score combination based on preferred column name patterns"""
    score = 0
    for col in col_combo:
        col_lower = col.lower()
        for pattern in prefer_patterns:
            if pattern in col_lower:
                score += 10
                break
        # Bonus for shorter column names
        score += max(0, 20 - len(col))
    return score
