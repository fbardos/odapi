# ODAPI - Open Data API

This is the repository containing the code for the Open Data API (ODAPI) project.
ODAPI has the target to collect open data from various sources and bundles them together in a simple API.
Contains data for Switzerland.

Currently focusing on:
- Indicators for municipalities, cantons and regions

General ELT:
- Dagster: Extract from URL to Minio
- Dagster: Load from Minio into Postgres Staging
- Transform using dbt

