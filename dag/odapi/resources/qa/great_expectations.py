import great_expectations as gx
import pandas as pd
from dagster import AssetCheckResult
from dagster import AssetCheckSeverity
from dagster import ConfigurableResource
from great_expectations.data_context.data_context.ephemeral_data_context import (
    EphemeralDataContext,
)
from great_expectations.data_context.types.base import DataContextConfig
from great_expectations.data_context.types.base import InMemoryStoreBackendDefaults
from great_expectations.expectations.expectation import Expectation


class GreatExpectationsResource(ConfigurableResource):

    def get_batch(self, data: pd.DataFrame):
        project_config = DataContextConfig(
            store_backend_defaults=InMemoryStoreBackendDefaults()
        )
        context = EphemeralDataContext(project_config=project_config)
        data_source = context.data_sources.add_pandas('pandas')
        data_asset = data_source.add_dataframe_asset(name='asset_check_df')
        batch_definition = data_asset.add_batch_definition_whole_dataframe(
            'batch_definition'
        )
        batch = batch_definition.get_batch(batch_parameters={'dataframe': data})
        return batch

    def run_expectation(
        self, data: pd.DataFrame, expectation: Expectation
    ) -> AssetCheckResult:
        result = self.get_batch(data).validate(expectation)
        assert isinstance(result.success, bool)
        return AssetCheckResult(
            passed=result.success,
            severity=AssetCheckSeverity.ERROR,
            metadata=result.result,
        )
