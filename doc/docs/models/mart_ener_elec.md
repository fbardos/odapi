# mart_ener_elec

Data about the electricity distributed in a municipality or canton, in a 15 minute interval.

Example Query:

```bash
curl -Gs 'https://odapi.bardos.dev/marts/mart_ener_elec' \
  --data-urlencode 'limit=5'
```

## Downstream Usage

- [Showcase Dashboard on HEX](https://app.hex.tech/01a0dfa8-c690-726b-8d22-c15fc6b38eba/app/martenerelec-034WuQ6CGYGcCMkKNBrn11/latest)

## Table Information

| Topic       | Value       |
| ----------- | ----------- |
| Primary Key | `zeitpunkt` |
| Language    | German      |

## Sources

| Publisher   | Level        | Information                                                                                                               |
| ----------- | ------------ | ------------------------------------------------------------------------------------------------------------------------- |
| Basel-Stadt | Canton       | [Dataset](https://opendata.swiss/de/dataset/kantonaler-stromverbrauch-netzlast)                                           |
| Winterthur  | Municipality | [Dataset](https://opendata.swiss/de/dataset/viertelstundenwerte-zum-bruttolastgang-elektrische-energie-der-stadt-zurich1) |
| Zurich      | Municipality | [Dataset](https://opendata.swiss/de/dataset/bruttolastgang-elektrische-energie-der-stadt-winterthur)                      |
