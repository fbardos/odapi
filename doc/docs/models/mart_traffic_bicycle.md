# mart_traffic_bicycle

Example Query:

```bash
curl -s 'https://odapi.bardos.dev/model/mart_traffic_bicycle'
```

Traffic statistics for bicycles, in different municipalities and cantons.

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
