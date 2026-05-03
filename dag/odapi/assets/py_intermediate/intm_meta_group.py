import pandas as pd
from dagster import AssetCheckResult
from dagster import AssetCheckSeverity
from dagster import AssetExecutionContext
from dagster import AssetKey
from dagster import asset
from dagster import asset_check
from great_expectations import expectations as gxe
from great_expectations.expectations.core.expect_table_row_count_to_be_between import (
    ExpectTableRowCountToBeBetween,
)
from sqlalchemy import text

from odapi.resources.postgres.postgres import PostgresResource
from odapi.resources.qa.great_expectations import GreatExpectationsResource
from odapi.utils.dbt_handling import load_data_models

MODEL_SEARCH_PATTERN = r'^(?!.*__\w+$)intm_.*$'


@asset(
    compute_kind='python',
    group_name='py_intermediate',
    key=['py_intermediate', 'intm_meta_group'],
    description=f"INTM model to dynamically compose SQL for selecting groups from INTM.",
    deps=[
        AssetKey(['intermediate', model.model_name])
        for model in load_data_models(
            pattern=MODEL_SEARCH_PATTERN, dbt_group='intermediate'
        )
    ],
)
def _asset(
    context: AssetExecutionContext,
    db: PostgresResource,
) -> pd.DataFrame:

    def build_query() -> str:
        selects = []
        for model in load_data_models(
            pattern=MODEL_SEARCH_PATTERN, dbt_group='intermediate'
        ):
            selects.append(f"""
                    select
                        jsonb_object_keys(grouping) as group_name
                    from {model.relation_name}\n
            """)

        return 'UNION \n'.join(selects)

    df = pd.read_sql(
        build_query(),
        db.get_sqlalchemy_engine(),
    )

    with db.get_sqlalchemy_engine().begin() as connection:
        connection.execute(text("""
                CREATE SCHEMA IF NOT EXISTS py_intermediate;
                CREATE TABLE IF NOT EXISTS py_intermediate.intm_meta_group (
                    group_id SMALLSERIAL,
                    group_name TEXT UNIQUE
                );
                """))

        # Write the DataFrame to the database
        for _, row in df.iterrows():
            connection.execute(
                text(
                    """
                    INSERT INTO py_intermediate.intm_meta_group (group_id, group_name)
                    VALUES (DEFAULT, :group_name)
                    ON CONFLICT (group_name) DO NOTHING;
                    """,
                ),
                {'group_name': row['group_name']},
            )

    return df


@asset_check(asset=_asset, blocking=True)
def ge_values_id_between_1_2000(
    great_expectations: GreatExpectationsResource,
    data: pd.DataFrame,
) -> AssetCheckResult:
    expectation = ExpectTableRowCountToBeBetween(
        min_value=1,
        max_value=2000,
    )
    return great_expectations.run_expectation(data, expectation)
