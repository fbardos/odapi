import pandas as pd
from dagster import AssetCheckResult
from dagster import AssetCheckSeverity
from dagster import AssetExecutionContext
from dagster import AssetKey
from dagster import asset
from dagster import asset_check
from great_expectations import expectations as gxe

from odapi.resources.postgres.postgres import PostgresResource
from odapi.resources.qa.great_expectations import GreatExpectationsResource
from odapi.utils.dbt_handling import load_intm_data_models


@asset(
    compute_kind='python',
    group_name='py_intermediate',
    key=['py_intermediate', 'intm_meta_group'],
    description=f"INTM model to dynamically compose SQL for selecting groups from INTM.",
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
                group_1_name
                , group_2_name
                , group_3_name
                , group_4_name
            from {model.relation_name}\n
        """
            for model in load_intm_data_models()
        ]
        src_select = 'UNION \n'.join(selects)
        return f"""
            with src as (
                {src_select}
            )

                select group_1_name::TEXT as group_name
                from src
                where group_1_name is not null
            UNION
                select group_2_name::TEXT as group_name
                from src
                where group_2_name is not null
            UNION
                select group_3_name::TEXT as group_name
                from src
                where group_3_name is not null
            UNION
                select group_4_name::TEXT as group_name
                from src
                where group_4_name is not null
        """

    df = pd.read_sql(
        build_query(),
        db.get_sqlalchemy_engine(),
    )

    with db.get_sqlalchemy_engine().begin() as connection:
        connection.execute(
            """
            CREATE SCHEMA IF NOT EXISTS py_intermediate;
            CREATE TABLE IF NOT EXISTS py_intermediate.intm_meta_group (
                group_id SMALLSERIAL,
                group_name TEXT UNIQUE
            );
        """
        )

        # Write the DataFrame to the database
        for _, row in df.iterrows():
            connection.execute(
                f"""
                INSERT INTO py_intermediate.intm_meta_group (group_id, group_name)
                VALUES (DEFAULT, %s)
                ON CONFLICT (group_name) DO NOTHING;
                """,
                row['group_name'],
            )

    return df


@asset_check(asset=_asset, blocking=True)
def ge_values_id_between_1_2000(
    great_expectations: GreatExpectationsResource,
    data: pd.DataFrame,
) -> AssetCheckResult:
    expectation = gxe.ExpectTableRowCountToBeBetween(
        min_value=1,
        max_value=2000,
    )
    result = great_expectations.get_batch(data).validate(expectation)
    assert isinstance(result.success, bool)

    return AssetCheckResult(
        passed=result.success,
        severity=AssetCheckSeverity.ERROR,
        metadata=result.result,
    )
