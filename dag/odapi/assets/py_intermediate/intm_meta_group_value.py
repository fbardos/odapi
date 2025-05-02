import pandas as pd
from dagster import AssetCheckResult
from dagster import AssetCheckSeverity
from dagster import AssetExecutionContext
from dagster import AssetKey
from dagster import DagsterError
from dagster import asset
from dagster import asset_check
from great_expectations import expectations as gxe

from odapi.resources.postgres.postgres import PostgresResource
from odapi.resources.qa.great_expectations import GreatExpectationsResource
from odapi.utils.dbt_handling import load_intm_data_models


@asset(
    compute_kind='python',
    group_name='py_intermediate',
    key=['py_intermediate', 'intm_meta_group_value'],
    description=f"INTM model to dynamically compose SQL for selecting group values from INTM.",
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
            f"""
            select
                group_1_value
                , group_2_value
                , group_3_value
                , group_4_value
            from {model.relation_name}\n
        """
            for model in load_intm_data_models()
        ]
        src_select = 'UNION \n'.join(selects)
        return f"""
            with src as (
                {src_select}
            )

                select group_1_value::TEXT as group_value
                from src
                where group_1_value is not null
            UNION
                select group_2_value::TEXT as group_value
                from src
                where group_2_value is not null
            UNION
                select group_3_value::TEXT as group_value
                from src
                where group_3_value is not null
            UNION
                select group_4_value::TEXT as group_value
                from src
                where group_4_value is not null
        """

    df = pd.read_sql(
        build_query(),
        db.get_sqlalchemy_engine(),
    )

    with db.get_sqlalchemy_engine().begin() as connection:
        connection.execute(
            """
            CREATE SCHEMA IF NOT EXISTS py_intermediate;
            CREATE TABLE IF NOT EXISTS py_intermediate.intm_meta_group_value (
                group_value_id SMALLSERIAL,
                group_value_name TEXT UNIQUE
            );
            INSERT INTO py_intermediate.intm_meta_group_value (group_value_id, group_value_name)
            VALUES (DEFAULT, 'GROUP TOTAL')
            ON CONFLICT (group_value_name) DO NOTHING;
        """
        )

        # Write the DataFrame to the database
        for idx, row in df.iterrows():
            context.log.debug(
                'Inserting group value: %s, on index %s', row['group_value'], idx
            )
            connection.execute(
                f"""
                INSERT INTO py_intermediate.intm_meta_group_value (group_value_id, group_value_name)
                VALUES (DEFAULT, %s)
                ON CONFLICT (group_value_name) DO NOTHING;
                """,
                row['group_value'],
            )

    return df


@asset_check(asset=_asset, blocking=True)
def ge_values_id_between_1_5000(
    great_expectations: GreatExpectationsResource,
    data: pd.DataFrame,
) -> AssetCheckResult:
    expectation = gxe.ExpectTableRowCountToBeBetween(
        min_value=1,
        max_value=5000,
    )
    result = great_expectations.get_batch(data).validate(expectation)
    assert isinstance(result.success, bool)

    return AssetCheckResult(
        passed=result.success,
        severity=AssetCheckSeverity.ERROR,
        metadata=result.result,
    )
