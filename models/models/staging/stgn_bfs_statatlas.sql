{%- set columns = dbt_utils.get_filtered_columns_in_relation(from=source('src', 'bfs_statatlas')) -%}
with src as (
    select
        "GEO_ID" as geo_id
        , "GEO_NAME" as geo_name
        , "VARIABLE" as variable
        , "VALUE" as value
        , "UNIT" as unit
        , "STATUS" as status
        , "STATUS_DESC" as status_desc
        , "DESC_VAL" as desc_val
        , "PERIOD_REF" as period_ref
        , "SOURCE" as source
        , "LAST_UPDATE"::DATE as last_update
        , "GEOM_CODE" as geom_code
        , "GEOM" as geom
        , "GEOM_PERIOD"::DATE as geom_period
        , "MAP_ID" as map_id
        , "MAP_URL" as map_url
        {% if 'mother_0_id' in columns %}
            , mother_0_id
            , mother_0_name 
        {% endif %}
        {% if 'mother_1_id' in columns %}
            , mother_1_id
            , mother_1_name 
        {% endif %}
        {% if 'mother_2_id' in columns %}
            , mother_2_id
            , mother_2_name 
        {% endif %}
        {% if 'mother_3_id' in columns %}
            , mother_3_id
            , mother_3_name 
        {% endif %}
        {% if 'mother_4_id' in columns %}
            , mother_4_id
            , mother_4_name 
        {% endif %}
        , CONCAT(''
            {% set re = modules.re %}
            {% for col in columns %}
                {% set is_match = re.match('mother_\d_id', col) %}
                {% if is_match %}
                    , '_' , {{ col }}
                {% endif %}
            {% endfor %}
        ) as mother_path
    from {{ source('src', 'bfs_statatlas') }}
)
, intm as (
    select
        *
        , {{ dbt_utils.generate_surrogate_key(['map_id', 'geo_id', 'variable', 'period_ref', 'geom_code', 'mother_path']) }} as _surrogate_key
    from src

)
select * from intm
