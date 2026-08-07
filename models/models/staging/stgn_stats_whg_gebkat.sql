with src as (
    select *
    from {{ source('src', 'statswiss_whg_gebkat') }}
)

, generate_grouping_signature as (
    select
        *
        , (
            select string_agg(
                outer_key || ':' || inner_key,
                '|' order by outer_key, inner_key
            )
            from jsonb_each(grouping) as g(outer_key, inner_obj)
            cross join lateral jsonb_object_keys(inner_obj) as k(inner_key)
        ) as grouping_signature
    from src
)

, generate_surrogate_key as (
    select
        *
        , {{ dbt_utils.generate_surrogate_key([
            'time_period',
            'diff_region_ref',
            'grouping_signature',
        ]) }} as sk
    from generate_grouping_signature
)

select
      structure             ::TEXT
    , structure_id          ::TEXT
    , action                ::TEXT
    , freq                  ::TEXT
    , time_period           ::SMALLINT
    , obs_value             ::NUMERIC
    , diff_region_state     ::DATE
    , diff_last_update      ::TIMESTAMP
    , diff_embargo_date     ::TIMESTAMP
    , diff_db_state         ::DATE
    , obs_status            ::TEXT
    , diff_region_ref       ::TEXT
    , source                ::TEXT
    -- remove empty name/parent
    -- , name                  
    -- , parent
    , grouping              ::JSONB
    , grouping_signature    ::TEXT
    , sk                    ::CHAR(32)
from generate_surrogate_key
