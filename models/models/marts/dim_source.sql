select distinct on (source) *
from {{ source('py_intermediate', 'intm_meta_source') }}
order by source, id
