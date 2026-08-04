select *
from {{ source('src', 'bl_downloads_portal') }}
