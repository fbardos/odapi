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
from odapi.utils.dbt_handling import load_intm_data_models


@asset(
    compute_kind='python',
    group_name='dbt_marts',
    key=['marts', 'mart_ogd_api'],
    description=f"Partitioned parent table for OGD API data.",
    deps=[
        AssetKey(['intermediate', model.model_name])
        for model in load_intm_data_models()
    ],
)
def _asset(
    context: AssetExecutionContext,
    db: PostgresResource,
) -> None:
    PARENT_SCHEMA = 'dbt_marts'
    PARENT_TABLE = 'mart_ogd_api'

    def indicators_per_model() -> dict[str, list[int]]:
        indicators_dict = {}
        for model in load_intm_data_models():
            _indicators = []
            _top_level_indicator = model.config.get('odapi', {}).get('indicator', None)
            _indicators.append(_top_level_indicator)
            _multiple_indicators = [
                i.get('indicator_id', None)
                for i in model.config.get('odapi', {}).get('indicators', [])
            ]
            _indicators.extend(_multiple_indicators)
            context.log.debug(f'All collected indicators: {_indicators}')
            _indicators = [i for i in _indicators if i is not None]
            context.log.debug(f'Filtered indicators: {_indicators}')
            indicators_dict[model.relation_name] = _indicators
        return indicators_dict

    def sql_parent_table() -> str:
        return f"""
            CREATE TABLE IF NOT EXISTS {PARENT_SCHEMA}.{PARENT_TABLE} (
                indicator_id            SMALLINT                        NULL
                , geo_code              CHAR(4)                         NULL
                , geo_value             SMALLINT                        NULL
                , knowledge_date_from   TIMESTAMP WITHOUT TIME ZONE     NULL
                , knowledge_date_to     TIMESTAMP WITHOUT TIME ZONE     NULL
                , period_type           TEXT                            NULL
                , period_code           TEXT                            NULL
                , period_ref_from       DATE                            NULL
                , period_ref            DATE                            NULL
                , group_1_name          TEXT                            NULL
                , group_1_value         TEXT                            NULL
                , group_2_name          TEXT                            NULL
                , group_2_value         TEXT                            NULL
                , group_3_name          TEXT                            NULL
                , group_3_value         TEXT                            NULL
                , group_4_name          TEXT                            NULL
                , group_4_value         TEXT                            NULL
                , indicator_value_numeric NUMERIC                       NULL
                , indicator_value_text    TEXT                          NULL
                , source                TEXT                            NULL
                , _etl_version          SMALLINT                        NULL
                , measure_code          TEXT                            NULL
            ) PARTITION BY LIST (indicator_id);
            """

    def sql_attach_partition() -> str:
        partitions_sql = ''
        for relation, indicators in indicators_per_model().items():
            indicators_list = ', '.join(map(str, indicators))
            if len(indicators) > 0:
                partitions_sql += f"""
                    ALTER TABLE {PARENT_SCHEMA}.{PARENT_TABLE}
                        ATTACH PARTITION {relation} FOR VALUES IN ({indicators_list});
                    """
        return partitions_sql

    def build_query() -> str:
        full_query = ''
        full_query += sql_parent_table()
        full_query += sql_attach_partition()
        context.log.info(f'Full SQL for parent table and partitions:\n{full_query}')
        return full_query

    with db.get_sqlalchemy_engine().begin() as connection:
        connection.execute(text(build_query()))
