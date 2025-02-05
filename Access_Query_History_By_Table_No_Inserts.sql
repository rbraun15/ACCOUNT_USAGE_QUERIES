-------------------------------------------------
 -------------------------------------------------
 --  Access & Query History By Table
 --     snowflake.account_usage.tables
 --     snowflake.account_usage.access_history
 --     snowflake.account_usage.query_history
 --       
 -- access_history - flattens base_objects_accessed to get table id, does not include INSERTS
 --  Query 3
 -------------------------------------------------
 -------------------------------------------------


-- Set variable accoring to what you are looking for:
SET db_name = 'QH_TESTING';
SET start_date = '2025-01-30 00:00:00';
SET end_date = '2025-02-02 23:00:00';
SET time_zone = 'America/New_York';

 
WITH 
-- GET OBJECT INFO
objects (database, schema, object_name, object_type, row_count, bytes, object_id) AS (
    SELECT
        table_catalog,
        table_schema,
        table_name,
        table_type,
        row_count,
        bytes,
        table_id    
    FROM
        snowflake.account_usage.tables
    WHERE  
        -- commented out line below b/c I was doing a lot of drop and re-create
        -- deleted IS NULL and 
        table_catalog IN ($db_name)
),
-- GET TABLES_ACCESSED INFO
access_history_flattened (query_id, object_id) AS (
    SELECT
        query_id,
        PARSE_JSON(lf.value):"objectId"::varchar   
    FROM
        snowflake.account_usage.access_history,
        LATERAL FLATTEN(base_objects_accessed) lf
    WHERE 
        CONVERT_TIMEZONE($time_zone, query_start_time) BETWEEN $start_date AND $end_date
        AND SPLIT(PARSE_JSON(lf.value):objectName, '.')[0]::varchar IN ($db_name)
),
-- GET QUERY HISTORY INFO
query_history_info AS (
    SELECT 
        query_id,
        query_type,
        query_text,
        start_time,
        user_name,
        rows_updated,
        rows_inserted,
        rows_deleted,
        partitions_total,
        partitions_scanned
    FROM 
        snowflake.account_usage.query_history  
    WHERE 
        database_name = $db_name 
        AND CONVERT_TIMEZONE($time_zone, start_time) BETWEEN $start_date AND $end_date
)
SELECT
    ah.query_id,
    o.database,
    o.schema,
    o.object_name AS table_name,
    ah.object_id,
    o.row_count,
    o.bytes/1024/1024/1024 as "Size_GB",
    qh.query_type,
    qh.query_text,
    qh.user_name,
    qh.start_time,
    qh.rows_inserted,
    qh.rows_updated,
    qh.rows_deleted,
    qh.partitions_total,
    qh.partitions_scanned
FROM
    objects o
    -- was outer joins to find objects with no activity, changing from left outer to just join
      JOIN access_history_flattened ah ON o.object_id = ah.object_id
      JOIN query_history_info qh ON ah.query_id = qh.query_id
WHERE 
    o.bytes > 0 
    AND ah.object_id IS NOT NULL
ORDER BY 
    ah.query_id, 
    o.database, 
    o.schema, 
    o.object_name;
