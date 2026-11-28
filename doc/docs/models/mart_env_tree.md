# mart_env_tree

Data about the tree register, maintained by authorities in Switzerland (Baumkataster).

Example Query:

```bash
curl -Gs 'https://odapi.bardos.dev/marts/mart_env_tree' \
  --data-urlencode 'limit=5'
```

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
