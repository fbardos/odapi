# mart_env_tree

Data about the tree register, maintained by authorities in Switzerland (Baumkataster).

Example Query:

```bash
curl -Gs 'https://odapi.bardos.dev/marts/mart_env_tree' \
  --data-urlencode 'limit=5'
```

## Downstream Usage

- [Showcase Dashboard on HEX](https://app.hex.tech/01a0dfa8-c690-726b-8d22-c15fc6b38eba/app/martenvtree-034wY8ju5746RTXVIEiG8N/latest)

## Table Information

| Topic       | Value  |
| ----------- | ------ |
| Primary Key | `id`   |
| Language    | German |

## Sources

| Publisher  | Level        | Information                                                                           |
| ---------- | ------------ | ------------------------------------------------------------------------------------- |
| Zug        | Canton       | [Dataset](https://services.geo.zg.ch/ows/baumkataster)                                |
| Winterthur | Municipality | [Dataset](https://www.geocat.ch/datahub/dataset/db0fddfb-614b-4bdf-99f9-dead621628e4) |
| Zurich     | Municipality | [Dataset](https://www.ogd.stadt-zuerich.ch/wfs/geoportal/Baumkataster)                |
