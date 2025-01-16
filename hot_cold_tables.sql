---------------------------------
 ---------------------------------
 --  Query History 365 Days
 ---------------------------------
 ---------------------------------
/*
What are the date ranges for query history and access history - 365 days
Per the docs - shows activity within the last year 
Link to docs - https://docs.snowflake.com/en/sql-reference/account-usage/query_history
               https://docs.snowflake.com/en/sql-reference/account-usage/access_history
*/

-- See the fist and last timestamp in your environment
 
select min(CONVERT_TIMEZONE('America/New_York', start_time)) as min_EST, max(CONVERT_TIMEZONE('America/New_York', start_time)) as max_EST  
from snowflake.account_usage.query_history  ;


 select min(CONVERT_TIMEZONE('America/New_York', query_start_time)) as min_EST, max(CONVERT_TIMEZONE('America/New_York', query_start_time)) as max_EST  
from snowflake.account_usage.access_history  ;



 ---------------------------------
 ---------------------------------
--
--  #1
--
--  Query History By Object
 --     snowflake.account_usage.tables
 --     snowflake.account_usage.access_history
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
 
-- Set variable accoring to what you are looking for:
SET db_name = 'SNOWDEV';
SET start_date = '2024-10-01 00:00:00';
SET end_date = '2025-01-08 23:00:00';
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
            deleted IS NULL
            AND table_catalog IN ($db_name) 
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
    ,ah.access_count as total_queries
    ,o.row_count
    ,o.bytes
    ,ah.first_accessed
    ,ah.last_accessed
FROM
    objects o
    LEFT OUTER JOIN access_history_flattened ah ON o.object_id = ah.object_id
WHERE object_type = 'BASE TABLE'
-- Modify order by to see "hot" or "cold" tables
-- order by TOTAL_QUERIES desc
ORDER BY database, schema, object_name;


 


 -------------------------------------------------
 -------------------------------------------------
--
--  #2
--
 --  Query History By Object
 --     snowflake.account_usage.tables
 --     snowflake.account_usage.access_history
 --     snowflake.account_usage.table_dml_history
 -------------------------------------------------
 -------------------------------------------------
  

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
            deleted IS NULL
            AND table_catalog IN ($db_name) 
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
    )   ,

 dml_activity (table_id, rows_added, rows_removed, rows_updated) AS (
    SELECT 
        table_id, 
        sum(rows_added), 
        sum(rows_removed), 
        sum(rows_updated) 
    FROM 
        snowflake.account_usage.table_dml_history dml
    WHERE
        CONVERT_TIMEZONE($time_zone, dml.start_time) BETWEEN $start_date AND $end_date
      GROUP BY
      table_id
)

    SELECT
    o.database
    ,o.schema
    ,o.object_name AS table_name
    ,ah.access_count as total_queries
    ,o.row_count
    ,o.bytes
    ,ah.first_accessed
    ,ah.last_accessed
    ,dml.rows_added
    ,dml.rows_removed
    ,dml.rows_updated
FROM
    objects o
    LEFT OUTER JOIN access_history_flattened ah ON o.object_id = ah.object_id    
    LEFT OUTER JOIN dml_activity dml ON o.object_id = dml.table_id
WHERE object_type = 'BASE TABLE'
-- Modify order by to see "hot" or "cold" tables
-- order by TOTAL_QUERIES desc
ORDER BY database, schema, object_name;



----
-- Random Stuff
----


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
            deleted IS NULL
            AND table_catalog IN ('SNOWDEV')
            and table_name = 'HIGHWAY_SAFETY';



 
            
   SELECT 
     * from
        snowflake.account_usage.table_dml_history

        Where 
        table_name = 'HIGHWAY_SAFETY'
        and 
        database_name= 'SNOWDEV';



 select table_id, schema_name, table_name, count(*) from snowflake.account_usage.table_dml_history
 where database_name= 'SNOWDEV'
 group by table_id ,schema_name,  table_name
 having count(*) > 1 
 order by 4 desc;



 describe table snowflake.account_usage.table_dml_history;
