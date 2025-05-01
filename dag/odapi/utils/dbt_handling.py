import json
from dataclasses import dataclass

from odapi.assets.dbt import dbt_manifest_path


@dataclass
class DbtModelSelection:
    model_name: str
    group: str
    relation_name: str


def load_intm_data_models() -> list[DbtModelSelection]:
    if not dbt_manifest_path.exists():
        raise FileNotFoundError(
            f"DBT manifest path does not exist: {dbt_manifest_path}"
        )

    with open(dbt_manifest_path, 'r') as file:
        manifest = json.load(file)

    _models = []
    for node_value in manifest['nodes'].values():

        # Loading INTM model from group intermediate
        if (
            node_value['resource_type'] == 'model'
            and node_value['group'] == 'intermediate'
            and node_value['name'].startswith('intm_')
        ):
            _models.append(
                DbtModelSelection(
                    model_name=node_value['name'],
                    group=node_value['group'],
                    relation_name=node_value['relation_name'],
                )
            )
    return _models
