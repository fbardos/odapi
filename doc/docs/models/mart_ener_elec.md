# mart_ener_elec

Example Query:

```bash
curl -s 'https://odapi.bardos.dev/model/mart_ener_elec'
```

Data about the electricity distributed in a municipality or canton, in a 15 minute interval.

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
