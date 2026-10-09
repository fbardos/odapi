CREATE USER public_api
IDENTIFIED WITH no_password
DEFAULT DATABASE data_marts
SETTINGS
    readonly = 1 READONLY,
    http_allow_table_as_file = 1 READONLY,
    http_allow_database_as_path = 1 READONLY,
    http_allow_filters_as_unrecognized_url_parameters = 0 READONLY,
    format_csv_null_representation = '' READONLY,
    format_tsv_null_representation = '' READONLY,
    default_format = 'TabSeparatedWithNames',
    output_format_parquet_geometadata = 1,
    max_execution_time = 120 READONLY,
    max_memory_usage = 2000000000 READONLY,
    max_threads = 2 READONLY,
    max_concurrent_queries_for_user = 4 READONLY;

GRANT SELECT ON data_marts.* TO public_api;
