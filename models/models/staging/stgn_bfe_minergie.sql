select
    "Municipality" as gemeinde
    , "BfsNumber"::INTEGER as gemeinde_bfs_id
    , "Minergie"::INTEGER as anzahl_minergie
    , "Minergie_Eco"::INTEGER as anzahl_minergie_eco
    , "Minergie_A"::INTEGER as anzahl_minergie_a
    , "Minergie_A_Eco"::INTEGER as anzahl_minergie_a_eco
    , "Minergie_P"::INTEGER as anzahl_minergie_p
    , "Minergie_P_Eco"::INTEGER as anzahl_minergie_p_eco
from {{ source('src', 'bfe_minergie') }}
