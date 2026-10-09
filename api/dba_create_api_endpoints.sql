CREATE HANDLER public_tables
URL '/tables'
METHODS (GET)
AS
SELECT
    name,
    comment
FROM system.tables
WHERE database = 'data_marts'
ORDER BY name;


CREATE HANDLER public_columns
URL '/columns'
METHODS (GET)
AS
SELECT
    name,
    type,
    comment
FROM system.columns
WHERE database = 'data_marts'
  AND table = {table:String}
ORDER BY position;
