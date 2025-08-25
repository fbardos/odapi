import great_expectations as gx
import pandas as pd
from dagster import AssetCheckResult
from dagster import AssetCheckSeverity
from dagster import AssetExecutionContext
from dagster import AssetKey
from dagster import DagsterError
from dagster import asset
from dagster import asset_check
from great_expectations import expectations as gxe
from sqlalchemy import text

from odapi.resources.postgres.postgres import PostgresResource
from odapi.resources.qa.great_expectations import GreatExpectationsResource
from odapi.utils.dbt_handling import load_intm_data_models


@asset(
    compute_kind='python',
    group_name='py_intermediate',
    key=['py_intermediate', 'intm_meta_source'],
    description=f"INTM model to dynamically compose SQL for selecting sources from INTM.",
    deps=[
        AssetKey(['intermediate', model.model_name])
        for model in load_intm_data_models()
    ],
)
def _asset(
    context: AssetExecutionContext,
    db: PostgresResource,
) -> pd.DataFrame:

    def build_query() -> str:
        selects = [
            f'select source from {model.relation_name}\n'
            for model in load_intm_data_models()
        ]
        src_select = 'UNION \n'.join(selects)
        return f"""
            with src as (
                {src_select}
            )
            select
                row_number() over ()::SMALLINT as id
                , source::TEXT
            from src
            group by source
        """

    df = pd.read_sql(
        build_query(),
        db.get_sqlalchemy_engine(),
    )

    # First, create schema and table if not exists.
    # Ensure, that the ID for the sources do not change over time.
    # To do this, set SERIAL for id, and add the constraint UNIQUE for source.
    # Do not insert row if it already exists.
    with db.get_sqlalchemy_engine().begin() as connection:
        connection.execute(
            text(
                """
                CREATE SCHEMA IF NOT EXISTS py_intermediate;
                CREATE TABLE IF NOT EXISTS py_intermediate.intm_meta_source (
                    id SMALLSERIAL,
                    source TEXT UNIQUE
                );
                """
            )
        )

        # Write the DataFrame to the database
        for _, row in df.iterrows():
            connection.execute(
                text(
                    f"""
                    INSERT INTO py_intermediate.intm_meta_source (id, source)
                    VALUES (DEFAULT, :source)
                    ON CONFLICT (source) DO NOTHING;
                    """
                ),
                {'source': row['source']},
            )

    return df


@asset_check(asset=_asset, blocking=True)
def ge_values_id_between_1_1000(
    great_expectations: GreatExpectationsResource,
    data: pd.DataFrame,
) -> AssetCheckResult:
    expectation = gxe.ExpectColumnValuesToBeBetween(
        column='id',
        min_value=1,
        max_value=1000,
    )
    result = great_expectations.get_batch(data).validate(expectation)
    assert isinstance(result.success, bool)

    return AssetCheckResult(
        passed=result.success,
        severity=AssetCheckSeverity.ERROR,
        metadata=result.result,
    )
