from dagster import load_assets_from_package_module
from dagster import Definitions
from dagster import EnvVar

import odapi.assets as assets
from odapi.assets.dbt import dbt
from odapi.resources.postgres.postgres import PostgresResource
from odapi.resources.postgres.postgres import XcomPostgresResource

from odapi.resources.ckan.ckan import OpenDataSwiss
from odapi.resources.minio.minio import Minio
from odapi.resources.crypto.fernet import FernetCipher
from odapi.resources.url.csv import KtzhGemeportraitUrlResource
from odapi.resources.extract.extract_handler import ExtractHandler
from odapi.resources.url.geojson import SwissboundariesTill2015
from odapi.resources.url.gpkg import Swissboundaries


import odapi.assets.ktzh.gemportrait as assets_ktzh_gp
import odapi.assets.bfs.statatlas as assets_bfs_statatlas
import odapi.assets.bfs.swissboundaries as assets_swissboundaries


################################################################################
# DEFINITIONS
################################################################################
# Currently, Definitions.merge() does not work properly. There is currently no
# possibility to merge resource definitions and the assets loaded from
# load_assets_from_package_module(). Will raise an exception, because the asset
# does not find the custom resource definitions.
# Also, cannot pack the resource, job and sensor definitions in the assets python
# file. Will not be recognized when using Dagster CLI to e.g. debug.
# For now, define all definitions here.
defs = Definitions(
    assets=load_assets_from_package_module(assets),
    resources={

        # Global resources
        'dbt': dbt,
        'db': PostgresResource(sqlalchemy_connection_string=EnvVar('POSTGRES__SQLALCHEMY_DATABASE_URI')),
        'xcom': XcomPostgresResource(sqlalchemy_connection_string=EnvVar('POSTGRES__SQLALCHEMY_DATABASE_URI')),
        'opendata_swiss': OpenDataSwiss(),
        'data_ktzh': KtzhGemeportraitUrlResource(),
        # do not make bucket name hardcoded already in the resource...
        'minio': Minio(
            endpoint_url=EnvVar('MINIO__ENDPOINT_URL'),
            bucket_name=EnvVar('MINIO__BUCKET_NAME'),
            access_key=EnvVar('MINIO__ACCESS_KEY'),
            secret_key=EnvVar('MINIO__SECRET_KEY'),
        ),
        'fernet': FernetCipher(fernet_key=EnvVar('FERNET_KEY')),
        'extractor': ExtractHandler(
            resource_minio=Minio(
                endpoint_url=EnvVar('MINIO__ENDPOINT_URL'),
                bucket_name=EnvVar('MINIO__BUCKET_NAME'),
                access_key=EnvVar('MINIO__ACCESS_KEY'),
                secret_key=EnvVar('MINIO__SECRET_KEY'),
            ),
            resource_fernet=FernetCipher(fernet_key=EnvVar('FERNET_KEY')),
        ),
        'geo_swissboundaries_till_2016': SwissboundariesTill2015(),
        'geo_swissboundaries': Swissboundaries(),
    },
    jobs=[
        *assets_ktzh_gp.jobs_gp,
        assets_bfs_statatlas.job_statatlas,
        assets_swissboundaries.job_bfs_swissboundaries,
    ],
    sensors=[
        *assets_ktzh_gp.sensors_gp,
    ],
    schedules=[
        assets_bfs_statatlas.schedule_statatlas,
        assets_swissboundaries.schedule_bfs_swissboundaries,
    ],
)
