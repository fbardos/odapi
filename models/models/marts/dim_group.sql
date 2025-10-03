select distinct on (group_name) *
from {{ source('py_intermediate', 'intm_meta_group') }}
order by group_name, group_id
