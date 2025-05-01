import great_expectations as ge
import pandas as pd
from dagster import AssetExecutionContext
from dagster import AssetKey
from dagster import DagsterError
from dagster import asset

from odapi.resources.postgres.postgres import PostgresResource
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
) -> None:

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

    # Great Expectations
    ge_context = ge.get_context()
    ge_data_source = ge_context.data_sources.add_pandas('pd')
    ge_data_asset = ge_data_source.add_dataframe_asset(name='intm_meta_group_value')
    ge_batch_definition = ge_data_asset.add_batch_definition_whole_dataframe(
        'batch definition'
    )
    batch = ge_batch_definition.get_batch(batch_parameters={'dataframe': df})
    expectation = ge.expectations.ExpectTableRowCountToBeBetween(
        min_value=1,
        max_value=5_000,
    )
    validation_result = batch.validate(expectation)
    if not validation_result.success:
        raise DagsterError(f"Validation failed for expectation: {validation_result}")

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
