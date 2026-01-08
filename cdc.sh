
--SETTING CDC CONFIGURATION ON DELTA TABLE IS A ONE TIME ACTIVITY, ONCE THE TABLE IS CREATED WE HAVE TO ALTER THE TABLE WITH CDC CHANGES SETTINGS

==========================================
RUN SPARK SUBMIT WITH BELOW CONFIGURATION
==========================================
spark3-shell --packages io.delta:delta-core_2.12:2.0.2 --conf "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension" --conf "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog"

=====================================
SET LMO TABLE WITH CDC CONFIGURATION
=====================================
spark.sql("alter table delta.`abfss://elevated@xxx.dfs.core.windows.net/dev/lmo/xxx/xxx/xxx` set TBLPROPERTIES (delta.enableChangeDataFeed = true)")

=====================================
SET EMO TABLE WITH CDC CONFIGURATION
=====================================
spark.sql("alter table delta.`abfss://elevated@xxx.dfs.core.windows.net/dev/xxx/xxx/xxx/xxx` set TBLPROPERTIES (delta.enableChangeDataFeed = true)")

=====================================
VERIFY CONFIGURATIOJN
=====================================
spark.sql("describe detail delta.`abfss://elevated@xxx.dfs.core.windows.net/dev/xxx/xxx/xxx/xxx`").show(false)
