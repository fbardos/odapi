-- some rows are exact duplicates (but with different statistics)
-- unify them before snapshot, will otherwise be difficult to
-- make historization
select
    datum
    , datensatz_id
    , publisher
    , any(datensatz_titel) as datensatz_titel -- can change in one day
    , ressourcen_id
    , any(ressourcen_titel) as ressourcen_titel -- can change in one day
    , download_url
    , sum(anzahl_klicks) as anzahl_klicks
    , sum(anzahl_besuchende) as anzahl_besuchende
    , sum(anzahl_downloads) as anzahl_downloads
    , array_agg(_record) as _records
    , _partition_datetime
    , file_source
from {{ source('src', 'ogd_usage_zh_resource') }}
-- TODO: propagate to others
group by all
