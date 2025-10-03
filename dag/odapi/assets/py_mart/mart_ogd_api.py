from dagster import AssetExecutionContext
from dagster import AssetKey
from dagster import asset
from great_expectations import expectations as gxe
from sqlalchemy import text

from odapi.resources.postgres.postgres import PostgresResource
from odapi.utils.dbt_handling import load_data_models


@asset(
    compute_kind='python',
    group_name='marts',
    key=['marts', 'mart_ogd_api'],
    description=f"Partitioned parent table for OGD API data.",
    deps=[
        AssetKey(['marts', model.model_name])
        for model in load_data_models(
            r'^(?!mart_ogd_api|mart_available_indicator)mart_.*$', dbt_group='marts'
        )
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
        # get the config from the corresponding intm models
        for model in load_data_models(
            r'^(?!.*__\w+$)intm_.*$', dbt_group='intermediate'
        ):
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
            # only models with at least one indicator config are relevant
            # filters out models like mart_available_indicator
            if len(_indicators) > 0:
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
                , group_1_id            SMALLINT                        NULL
                , group_value_1_id      SMALLINT                        NULL
                , _group_value_1_is_total   BOOLEAN                     NULL
                , group_2_id            SMALLINT                        NULL
                , group_value_2_id      SMALLINT                        NULL
                , _group_value_2_is_total   BOOLEAN                     NULL
                , group_3_id            SMALLINT                        NULL
                , group_value_3_id      SMALLINT                        NULL
                , _group_value_3_is_total   BOOLEAN                     NULL
                , group_4_id            SMALLINT                        NULL
                , group_value_4_id      SMALLINT                        NULL
                , _group_value_4_is_total   BOOLEAN                     NULL
                , indicator_value_numeric NUMERIC(32,9)                 NULL
                , indicator_value_text  TEXT                            NULL
                , measure_code          TEXT                            NULL
                , source_id             SMALLINT                        NULL
            ) PARTITION BY LIST (indicator_id);
            """

    def sql_func_attach_partition() -> str:
        return f"""
            CREATE OR REPLACE FUNCTION attach_partition_if_missing(
              parent  regclass,
              child   regclass,
              bounds  text  -- e.g. 'FOR VALUES IN (1,2,3)' or 'FOR VALUES FROM (...) TO (...)'
            ) RETURNS void
            LANGUAGE plpgsql AS $$
            BEGIN
              IF NOT EXISTS (
                SELECT 1 FROM pg_inherits
                WHERE inhrelid = child AND inhparent = parent
              ) THEN
                EXECUTE format('ALTER TABLE %s ATTACH PARTITION %s %s', parent, child, bounds);
              END IF;
            END$$;
        """

    def sql_attach_partition() -> str:
        partitions_sql = ''
        for relation, indicators in indicators_per_model().items():
            # replace relation to mart (not from intm, because view)
            relation = relation.replace('intm_', 'mart_')
            relation = relation.replace('intermediate', 'marts')
            indicators_list = ', '.join(map(str, indicators))
            if len(indicators) > 0:
                partitions_sql += f"""
                    SELECT attach_partition_if_missing(
                      '{PARENT_SCHEMA}.{PARENT_TABLE}'::REGCLASS,
                      '{relation}'::REGCLASS,
                      'FOR VALUES IN ({indicators_list})'
                    );
                """
        return partitions_sql

    def build_query() -> str:
        full_query = ''
        full_query += sql_parent_table()
        full_query += sql_func_attach_partition()
        full_query += sql_attach_partition()
        context.log.info(f'Full SQL for parent table and partitions:\n{full_query}')
        return full_query

    with db.get_sqlalchemy_engine().begin() as connection:
        connection.execute(text(build_query()))
