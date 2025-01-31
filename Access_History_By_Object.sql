---------------------------------
 ---------------------------------
 --  Access History By Object
 --     snowflake.account_usage.tables
 --     snowflake.account_usage.access_history
 --  Query 1
 ---------------------------------
 ---------------------------------
 /*
This query summarizes access info by table for specified database.  It shows the following items:

- Database
- Schema
- Table
- Total Queries = # queries that included the table in specified date range, if not queried will be null
- Row Count = current number of recods in the table
- Bytes = current size of the table
- First_accessed = first date table was queried in specified time range
- Last_accessed = last date table was queried in specified time range

Tables used:
- snowflake.account_usage.tables to get current row counts and size for specified db.
- snowflake.account_usage.access_history to get first/last access date, # of queryies, and object names
*/
 
-- Set variable according to what you are looking for:
SET db_name = 'SNOWDEV';
SET start_date = '2024-10-01 00:00:00';
SET end_date = '2025-01-31 23:59:59';
SET time_zone = 'America/New_York';




 WITH objects ( database, schema, object_name, object_type, row_count, bytes, object_id) AS (
        SELECT
            table_catalog 
            ,table_schema 
            ,table_name 
            ,table_type
            ,row_count
            ,bytes
            ,table_id    
        FROM
            snowflake.account_usage.tables
        WHERE
            -- deleted IS NULL and
            table_catalog IN ($db_name) 
    ),
access_history_flattened (object_id, first_accessed, last_accessed, access_count) AS (
        SELECT
            PARSE_JSON(lf.value):"objectId"::varchar 
            ,MIN(query_start_time::date)
            ,MAX(query_start_time::date)
            ,COUNT(*)
        FROM
            snowflake.account_usage.access_history ah,
            LATERAL FLATTEN(base_objects_accessed) lf
        WHERE CONVERT_TIMEZONE($time_zone, ah.query_start_time) BETWEEN $start_date AND $end_date
        AND SPLIT(PARSE_JSON(lf.value):objectName, '.')[0]::varchar  IN ($db_name)
        GROUP BY 1
    )    

    SELECT
    o.database
    ,o.schema
    ,o.object_name AS table_name
    ,o.object_id
    ,o.row_count
    ,o.bytes/1024/1024/1024 as "Size_GB"
     ,ah.access_count as total_queries
    ,ah.object_id
    ,ah.first_accessed
    ,ah.last_accessed
FROM
    objects o
    LEFT OUTER JOIN access_history_flattened ah ON o.object_id = ah.object_id
WHERE object_type = 'BASE TABLE'
-- Modify order by to see "hot" or "cold" tables
-- order by TOTAL_QUERIES desc
ORDER BY database, schema, object_name;
