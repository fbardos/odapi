# mart_traffic_miv

Traffic statistics for motorized vehicles, following the [SWISS10-Definition](https://www.astra.admin.ch/dam/astra/de/dokumente/standards_fuer_nationalstrassen/astra_13012_verkehrszaehler2009v105.pdf.download.pdf/astra_13012_verkehrszaehler.pdf) in different municipalities and cantons.

/// note | TL;DR

- Combined data from Canton Basel-Stadt, City of Zurich and Winterthur
- Hourly traffic count per category.
  ///

Example Query:

```bash
curl -Gs 'https://odapi.bardos.dev/marts/mart_traffic_miv'
  --data-urlencode 'limit=5'
```

## Downstream Usage

- [Showcase Dashboard on HEX](https://app.hex.tech/01a0dfa8-c690-726b-8d22-c15fc6b38eba/app/marttrafficmiv-034VjNNgWmJtvAM3066e9N/latest)

## Table Information

| Topic       | Value                                                         |
| ----------- | ------------------------------------------------------------- |
| Primary Key | Combination of `anlage_id`, `richtung`, `spur` and `zeit_von` |
| Language    | German                                                        |

## Sources

| Publisher   | Level        | Information                                                                                                                |
| ----------- | ------------ | -------------------------------------------------------------------------------------------------------------------------- |
| Basel-Stadt | Canton       | [Dataset](https://opendata.swiss/de/dataset/verkehrszahldaten-motorisierter-individualverkehr)                             |
| Thurgau     | Canton       | [Dataset](https://opendata.swiss/de/dataset/verkehrszahldaten-motorisierter-individualverkehr-nach-fahrzeugklassen)        |
| St.Gallen   | Municipality | [Dataset](https://opendata.swiss/de/dataset/verkehrszahlung-miv-stadt-st-gallen-nach-fahrzeugkategorien-swiss10-2019-2022) |
| Winterthur  | Municipality | [Dataset](https://opendata.swiss/de/dataset/verkehrszahldaten-motorisierter-individualverkehr-in-winterthur)               |
