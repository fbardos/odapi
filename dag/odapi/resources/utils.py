from dagster import ConfigurableResource
from dagster import AssetExecutionContext
from dagster import AssetKey

    
def get_metadata_from_latest_materialization(context: AssetExecutionContext, asset_key: str, metadata_key: str) -> str:
    materialization_event = context.instance.get_latest_materialization_event(AssetKey(asset_key))
    assert materialization_event is not None, 'No materialization event found'
    materialization = materialization_event.asset_materialization
    assert materialization is not None, 'No materialization found'
    return str(materialization.metadata[metadata_key].text)

def calculate_bytes_compression(bytes_before: int, bytes_after: int) -> float:
    return bytes_after / bytes_before
