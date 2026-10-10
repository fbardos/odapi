-- I suspect, some rows are currently duplicates among the dataset.
-- On a given date and dataset_id, the visitors and interactions are
-- identical. When SELECT DISTINCT ON, with visitors and interactions
-- i get rid of these duplicates, when even visitors and interactions
-- are the same.
select distinct on (dataset_id, date, visitors, interactions) *
from {{ source('src', 'ogd_usage_bl') }}
order by
    dataset_id
    , date
    , visitors
    , interactions
    , _record
