import pandas as pd
from dagster import AssetCheckResult
from dagster import AssetCheckSeverity
from dagster import AssetExecutionContext
from dagster import AssetKey
from dagster import DagsterError
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
    key=['py_intermediate', 'intm_meta_group_value'],
    description=f"INTM model to dynamically compose SQL for selecting group values from INTM.",
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
                    inner_kv.value as group_value_name
                from {model.relation_name} t
                    cross join lateral jsonb_each(t.your_jsonb_column) AS outer_kv(key, value)
                    cross join lateral jsonb_each(outer_kv.value) AS inner_kv(key, value)\n
            """)

        return 'UNION \n'.join(selects)

    df = pd.read_sql(
        build_query(),
        db.get_sqlalchemy_engine(),
    )

    with db.get_sqlalchemy_engine().begin() as connection:
        connection.execute(text("""
                CREATE SCHEMA IF NOT EXISTS py_intermediate;
                CREATE TABLE IF NOT EXISTS py_intermediate.intm_meta_group_value (
                    group_value_id SMALLSERIAL,
                    group_value_name TEXT UNIQUE
                );
                INSERT INTO py_intermediate.intm_meta_group_value (group_value_id, group_value_name)
                VALUES (DEFAULT, 'GROUP TOTAL')
                ON CONFLICT (group_value_name) DO NOTHING;
                """))

        # Write the DataFrame to the database
        for idx, row in df.iterrows():
            context.log.debug(
                'Inserting group value: %s, on index %s', row['group_value'], idx
            )
            connection.execute(
                text(f"""
                    INSERT INTO py_intermediate.intm_meta_group_value (group_value_id, group_value_name)
                    VALUES (DEFAULT, :group_value)
                    ON CONFLICT (group_value_name) DO NOTHING;
                    """),
                {'group_value': row['group_value']},
            )

    return df


@asset_check(asset=_asset, blocking=True)
def ge_values_id_between_1_5000(
    great_expectations: GreatExpectationsResource,
    data: pd.DataFrame,
) -> AssetCheckResult:
    expectation = ExpectTableRowCountToBeBetween(
        min_value=1,
        max_value=5000,
    )
    return great_expectations.run_expectation(data, expectation)
