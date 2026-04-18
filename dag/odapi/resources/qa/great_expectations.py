import great_expectations as gx
import pandas as pd
from dagster import AssetCheckResult
from dagster import AssetCheckSeverity
from dagster import ConfigurableResource
from great_expectations.expectations.expectation import Expectation


class GreatExpectationsResource(ConfigurableResource):

    def get_batch(self, data: pd.DataFrame):
        context = gx.get_context(mode='ephemeral')
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
