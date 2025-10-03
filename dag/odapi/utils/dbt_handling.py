import json
import re
from dataclasses import dataclass
from typing import Optional

from odapi.assets.dbt import dbt_manifest_path


@dataclass
class DbtModelSelection:
    model_name: str
    group: str
    relation_name: str
    config: dict


def load_data_models(
    pattern: Optional[str] = None, dbt_group: Optional[str] = None
) -> list[DbtModelSelection]:
    """Load DBT models from the manifest.json file.

    Args:
        pattern: Regex pattern to filter model names. Defaults to None.
        dbt_group: DBT group to filter models. Defaults to None.
    """
    if not dbt_manifest_path.exists():
        raise FileNotFoundError(
            f"DBT manifest path does not exist: {dbt_manifest_path}"
        )

    with open(dbt_manifest_path, 'r') as file:
        manifest = json.load(file)

    _models = []
    for node_value in manifest['nodes'].values():

        # Loading INTM model from group intermediate
        if node_value['resource_type'] == 'model':
            if pattern and re.match(pattern, node_value['name']) is None:
                continue
            if dbt_group and node_value['group'] != dbt_group:
                continue
            _models.append(
                DbtModelSelection(
                    model_name=node_value['name'],
                    group=node_value['group'],
                    relation_name=node_value['relation_name'],
                    config=node_value['config'],
                )
            )
    return _models
