select
    src.gemeinde_name
    , src.total
    , src.source
    {% set age_groups = [
        'age0_4',
        'age5_9',
        'age10_14',
        'age15_19',
        'age20_24',
        'age25_29',
        'age30_34',
        'age35_39',
        'age40_44',
        'age45_49',
        'age50_54',
        'age55_59',
        'age60_64',
        'age65_69',
        'age70_74',
        'age75_79',
        'age80_84',
        'age85_89',
        'age90_94',
        'age95_99',
        'age100_plus',
    ] %}
    -- when no inhabitants are present in an age group, the value is '–'
    {% for age_group in age_groups %}
        , case 
            when src.{{ age_group }}::TEXT = '–' then 0
            else src.{{ age_group }}::INTEGER
        end as {{ age_group }}
    {% endfor %}
    , src.year
    , {{ dbt_utils.generate_surrogate_key(['year', 'gemeinde_name']) }} as _surrogate_key
from {{ source('src', 'sss_bevoelkerung_altersklassen')}} src
