import os
from pathlib import Path
from dagster import AssetExecutionContext
from dagster_dbt import DbtCliResource
from dagster_dbt import DbtProject
from dagster_dbt import dbt_assets
from dotenv import load_dotenv
from dagster import get_dagster_logger

################################################################################
# DBT
################################################################################
load_dotenv()

dbt_path_str = os.getenv('DBT__ABSOLUTE_PATH')
assert dbt_path_str is not None
dbt_path = Path(dbt_path_str).resolve()
dbt_project = DbtProject(project_dir=dbt_path)

dbt = DbtCliResource(project_dir=dbt_project)

if os.getenv('DAGSTER_DBT_PARSE_PROJECT_ON_LOAD'):
    logger = get_dagster_logger()
    logger.info('Parsing dbt project, because ENV DAGSTER_DBT_PARSE_PROJECT_ON_LOAD is set...')
    dbt_manifest_path = (
        dbt.cli(
            ["--quiet", "parse"],
            target_path=Path("target")
        )
        .wait()
        .target_path.joinpath('manifest.json')
    )
else:
    assert dbt_path is not None
    dbt_manifest_path = Path(dbt_path).joinpath("target", "manifest.json")


@dbt_assets(
    manifest=dbt_manifest_path,
)
def assets_homelab_dbt(context: AssetExecutionContext, dbt: DbtCliResource):
    yield from dbt.cli(["build"], context=context).stream()

