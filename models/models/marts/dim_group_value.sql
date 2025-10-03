select distinct on (group_value_name) *
from {{ source('py_intermediate', 'intm_meta_group_value') }}
order by group_value_name, group_value_id
