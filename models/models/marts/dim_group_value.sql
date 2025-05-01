select *
from {{ source('py_intermediate', 'intm_meta_group_value') }}
