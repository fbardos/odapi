from dagster import asset
from dagster import AssetExecutionContext
from dagster import MetadataValue
from dagster import define_asset_job
from dagster import ScheduleDefinition
from odapi.resources.ckan.ckan import OpenDataSwiss
from odapi.resources.postgres.postgres import PostgresResource
from dataclasses import dataclass
import pandas as pd
import numpy as np
import datetime as dt


@dataclass
class MunicipalityResource:
    run_timestamp: dt.datetime
    organization_id: str
    package_id: str
    resource_id: str
    resource_format: str
    resource_url: str
    resource_preview: str

@asset(
    compute_kind='python',
    group_name='analytics',
    key=['src', 'detect_resources'],
)
def detect_resources(
    context: AssetExecutionContext,
    opendata_swiss: OpenDataSwiss,
    db: PostgresResource,
):
    run_timestamp = dt.datetime.now()
    federal_orgs = list(opendata_swiss.get_federal_organization_ids())
    municipality_resources = []

    # Iterate federal orgs
    for org in federal_orgs:
        context.log.debug(f'Getting packages for organization {org}...')
        for package in opendata_swiss.get_packages_by_organization_id(org):
            _package_id = package.get('id', None)
            for resource in package.get('resources', []):
                _resource_id = resource.get('id', None)
                _resource_format = resource.get('format', '').lower()
                _resource_url = resource.get('url', '')
                if _resource_format == 'csv':
                    context.log.debug(f'Processing resource {_resource_id} with format {_resource_format}')
                    try:
                        # Many CSV files are not properly formatted, so we skip bad lines
                        df = pd.read_csv(_resource_url, on_bad_lines='warn')
                    except Exception as e:
                        context.log.error(f'Error loading CSV {_resource_url}: {e}')
                        continue

                    # column stack to search for one of the strings in any of the columns
                    mask = np.column_stack([df[col].astype(str).str.contains(r'Winterthur|Bern|Lugano', na=False, case=False, regex=True) for col in df])
                    if len(df.loc[mask.any(axis=1)]) > 0:
                        print(f'Found data for Winterthur, Bern or Lugano in URL {_resource_url}')
                        municipality_resources.append(MunicipalityResource(
                            run_timestamp=run_timestamp,
                            organization_id=org,
                            package_id=_package_id,
                            resource_id=_resource_id,
                            resource_format=_resource_format,
                            resource_url=_resource_url,
                            resource_preview=df.head().to_string(),
                        ))
                    else:
                        print(f'No data found for Winterthur, Bern or Lugano in URL {_resource_url}, skipping...')

                else:
                    context.log.debug(f'Skipping resource {_resource_id} with format {_resource_format}')

    # When done, insert the data into the database
    if len(municipality_resources) > 0:
        context.log.debug(f'Inserting {len(municipality_resources)} records into the database...')
        df_insert = pd.DataFrame(municipality_resources)
        df_insert.to_sql(
            'municipality_resources',
            con=db.get_sqlalchemy_engine(),
            schema='analytics',
            if_exists='append',
            index=False,
        )

        # Add metadata
        context.add_output_metadata(metadata={
            'num_records': len(df_insert.index),
            'num_cols': len(df_insert.columns),
            'preview': MetadataValue.md(df_insert.head().to_markdown()),
        })

job = define_asset_job(
    name='job_detect_resources',
    selection=['src/detect_resources'],
)

schedule = ScheduleDefinition(
    job=job,
    cron_schedule="5 4 4 * *",
)
