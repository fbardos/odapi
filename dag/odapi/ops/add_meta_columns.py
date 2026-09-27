import re
from datetime import datetime
from datetime import timezone

import polars as pl
from dagster import AssetExecutionContext

_TIMESTAMP_RE = re.compile(
    r'(?P<timestamp>\d{4}-\d{2}-\d{2}T\d{2}-\d{2}-\d{2}Z)'
)


def partition_datetime(context: AssetExecutionContext) -> datetime:
    match = _TIMESTAMP_RE.search(context.partition_key)

    if match is None:
        raise ValueError(
            f'No timestamp found in partition key: {context.partition_key!r}'
        )

    return datetime.strptime(
        match.group('timestamp'),
        '%Y-%m-%dT%H-%M-%SZ',
    ).replace(tzinfo=timezone.utc)


def add_meta_columns(
    df: pl.DataFrame,
    context: AssetExecutionContext,
    record_offset: int = 0,
    **columns: object,
) -> pl.DataFrame:
    return df.with_columns(
        pl.int_range(
            record_offset + 1,
            record_offset + df.height + 1,
            eager=True,
        ).alias('_record'),
        pl.lit(partition_datetime(context)).alias('_partition_datetime'),
        *[pl.lit(value).alias(name) for name, value in columns.items()],
    )
