select
	variable
	, count(*) as rows
	, count(distinct geo_id) as distinct_gem
from snapshots.snap_bfs_statatlas
where variable ~ '^Lebendgeburten pro 1.?000'
	and geom_code = 'polg'
group by 1



SELECT
	variable
	, period_ref
	, count(*)
FROM snapshots.snap_bfs_statatlas
where variable ~ 'Anteil der Wasser- und Feuchtflächen\* \(Wasser, Gletscher, Nassstandorte\) an der Gesamtfläche'
	and geom_code = 'polg'
group by 1, 2
order by 2 desc


SELECT * FROM snapshots.snap_bfs_statatlas
where variable ~ '^Anzahl Einwohner/innen$'
	and geom_code = 'polg'
	and geo_id = '230'
LIMIT 100

