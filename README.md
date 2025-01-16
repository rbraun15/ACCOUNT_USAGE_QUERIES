# See Tables and Activity

Leverages Snowflake Account_Usage data to identify tables with high and low activity.  

See metrics such as table  name, rowcount, number of queries, date of first/last access, # rows added/removed/updated.

**There are primary queries:**
- **#1)** - snowflake.account_usage.tables and snowflake.account_usage.access_history
-  **#2)** - snowflake.account_usage.tables, snowflake.account_usage.access_history and snowflake.account_usage.table_dml_history
