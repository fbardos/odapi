# mart_traffic_bicycle

Traffic statistics for bicycles, in different municipalities and cantons.

Example Query:

```bash
curl -Gs 'https://odapi.bardos.dev/marts/mart_traffic_bicycle' \
  --data-urlencode 'limit=5'
```

## Downstream Usage

- [Showcase Dashboard on HEX](https://app.hex.tech/01a0dfa8-c690-726b-8d22-c15fc6b38eba/app/Velo-data-fetch-API-034Wsy6pKetQW6IBFLWuK7/latest)

## Table Information

| Topic       | Value                                                              |
| ----------- | ------------------------------------------------------------------ |
| Primary Key | Combination of `standort_id`, `zeit_von`, `richtung` and `spur_id` |
| Language    | German                                                             |

## Sources

| Publisher   | Level        | Information                                                                                                         |
| ----------- | ------------ | ------------------------------------------------------------------------------------------------------------------- |
| Basel-Stadt | Canton       | [Dataset](https://opendata.swiss/de/dataset/verkehrszahldaten-velos-und-fussganger)                                 |
| Winterthur  | Municipality | [Dataset](https://opendata.swiss/de/dataset/verkehrszahldaten-veloverkehr-in-winterthur)                            |
| Zurich      | Municipality | [Dataset](https://opendata.swiss/de/dataset/daten-der-automatischen-fussganger-und-velozahlung-viertelstundenwerte) |
