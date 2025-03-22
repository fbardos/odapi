-- depends_on: {{ ref('dim_gemeinde_latest') }}
{{ stgn_bfs_stat_tab(source('src', 'stat_tab_wirtschaft_unternehmen')) }}

select * from final
