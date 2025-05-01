import great_expectations as gx
import pandas as pd
from dagster import ConfigurableResource


class GreatExpectationsResource(ConfigurableResource):

    def get_batch(self, asset_df: pd.DataFrame):
        context = gx.get_context(mode='ephemeral')
        data_source = context.data_sources.add_pandas('pandas')
        data_asset = data_source.add_dataframe_asset(name='asset_check_df')
        batch_definition = data_asset.add_batch_definition_whole_dataframe(
            'batch_definition'
        )
        batch = batch_definition.get_batch(batch_parameters={'dataframe': asset_df})
        return batch
