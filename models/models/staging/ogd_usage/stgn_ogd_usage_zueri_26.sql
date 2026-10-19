select *
from {{ source('src', 'ogd_usage_zueri_26') }}
