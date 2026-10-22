# mart_ogd_usage

Visits and Downloads from Open Data catalogues, from different municipalities and
cantons. This data can be used for e.g. calculating the public interest in different
topics.

Example Query:

```bash
curl -Gs 'https://odapi.bardos.dev/marts/mart_ogd_usage' \
  --data-urlencode 'limit=5'
```

## Downstream Usage

- [Showcase Dashboard on HEX](https://app.hex.tech/01a0dfa8-c690-726b-8d22-c15fc6b38eba/app/martogdusage-034kD6dp4pVrSMcvst4c9o/latest)

## Table Information

| Topic       | Value                                |
| ----------- | ------------------------------------ |
| Primary Key | Combination: `date` and `dataset_id` |
| Language    | German                               |

## Sources

| Publisher | Level        | Information                                                                                          |
| --------- | ------------ | ---------------------------------------------------------------------------------------------------- |
| Zurich    | municipality | [Dataset](https://opendata.swiss/de/dataset/download-zahlen-des-open-data-katalogs-der-stadt-zurich) |

## Definitions

The defintions for `downloads` and `visits` differ per publisher quite a bit.
Be careful, when comparing these statistics.

| Publisher  | Level | Show robots | Deepest Level | downloads              |
| ---------- | ----- | ----------- | ------------- | ---------------------- |
| Zurich     | MUN   | yes         | Resource      | downloads              |
| Zurich     | CAN   | yes         | Resource      | downloads              |
| Basel-Land | CAN   | no          | Dataset       | downloads+interactions |
