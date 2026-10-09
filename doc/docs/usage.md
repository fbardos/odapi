# Usage

This API uses the [HTTP interface from Clickhouse](https://clickhouse.com/docs/concepts/features/interfaces/http).
This allows filters and the control of the output format.

## Start

### List of tables

The published tables can be found with:

```bash
curl -s 'https://odapi.bardos.dev/tables'
```

### List of columns

The columns of a table can be listed with:

```bash
curl -Gs 'https://odapi.bardos.dev/columns' \
  --data-urlencode 'table=mart_traffic_bicycle'
```

### Get data from a table

```bash
curl -Gs 'https://odapi.bardos.dev/marts/mart_traffic_bicycle' \
  --data-urlencode 'limit=5'
```

Where:

- `limit=5` will only return the first 5 rows. Default: all rows

## File format

Clickhouse offers a big variety of
[output formats](https://clickhouse.com/docs/reference/formats).
You can control, which data format you like, to work with later.

Here are a few examples:

### Export to CSV

Just add the filetype at the end of the table name:

```bash
curl -Gs 'https://odapi.bardos.dev/marts/mart_traffic_bicycle.csv' \
  --data-urlencode 'limit=5'
```

The default is a CSV without column names. If you need the column names, you can
also get more control over the file structure with `format=...`:

```bash
curl -Gs 'https://odapi.bardos.dev/marts/mart_traffic_bicycle' \
  --data-urlencode 'limit=5' \
  --data-urlencode 'format=CSVWithNames'
```

### Export to Parquet

Per default, the API will return a GeoParquet instead of a regular Parquet file:

```bash
curl -Gs 'https://odapi.bardos.dev/marts/mart_traffic_bicycle.parquet' \
  --data-urlencode 'limit=5'
```

### Export to JSON

Per default, will return a normal JSON file:

```bash
curl -Gs 'https://odapi.bardos.dev/marts/mart_traffic_bicycle.json' \
  --data-urlencode 'limit=5'
```

But, you can also return a GeoJSON:

```bash
curl -Gs 'https://odapi.bardos.dev/marts/mart_traffic_bicycle' \
  --data-urlencode 'limit=5' \
  --data-urlencode 'format=GeoJSON'
```

## Filtering

You can control the SQL WHERE part of the query with the `filter` keyword. Examples:

Get only rows with `standort_id==1037`:

```bash
curl -Gs 'https://odapi.bardos.dev/marts/mart_traffic_bicycle' \
  --data-urlencode 'limit=5' \
  --data-urlencode "filter=standort_id = '1037'"
```

Or rows with more than 10 counted velos in a 15-min-interval:

```bash
curl -Gs 'https://odapi.bardos.dev/marts/mart_traffic_bicycle' \
  --data-urlencode 'limit=5' \
  --data-urlencode "filter=velos > 10"
```

## Sorting

You can custom sort the rows with `sort`:

```bash
curl -Gs 'https://odapi.bardos.dev/marts/mart_traffic_bicycle' \
  --data-urlencode 'limit=5' \
  --data-urlencode "sort=zeit_von, standort_id"
```

And if you want to sort descending, just add `-` before the column name:

```bash
curl -Gs 'https://odapi.bardos.dev/marts/mart_traffic_bicycle' \
  --data-urlencode 'limit=5' \
  --data-urlencode "sort=-zeit_von"
```

## Select columns

If you dont need all columns, select them specifically with `select`:

```bash
curl -Gs 'https://odapi.bardos.dev/marts/mart_traffic_bicycle' \
  --data-urlencode 'limit=5' \
  --data-urlencode "select=standort_id,zeit_von,velos"
```

You can also SELECT EXCEPT with:

```bash
curl -Gs 'https://odapi.bardos.dev/marts/mart_traffic_bicycle' \
  --data-urlencode 'limit=5' \
  --data-urlencode "select=* EXCEPT (file_source, publisher)"
```
