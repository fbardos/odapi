-- depends_on: {{ ref('dim_gemeinde_latest') }}
{{ stgn_bfs_stat_tab(source('src', 'stat_tab_geb_leerwhg')) }}

select * from final
