-- Databricks notebook source
-- MAGIC %md
-- MAGIC ## Creating Catalog

-- COMMAND ----------

CREATE CATALOG IF NOT EXISTS anshlamba_yt_project
    MANAGED LOCATION 'abfss://anshlamba-yt-project@anshlambaprojectextdl.dfs.core.windows.net/';

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Creating Schemas

-- COMMAND ----------

CREATE SCHEMA IF NOT EXISTS anshlamba_yt_project.landing;
CREATE SCHEMA IF NOT EXISTS anshlamba_yt_project.checkpoints;
CREATE SCHEMA IF NOT EXISTS anshlamba_yt_project.bronze
    MANAGED LOCATION 'abfss://anshlamba-yt-project@anshlambaprojectextdl.dfs.core.windows.net/bronze';
CREATE SCHEMA IF NOT EXISTS anshlamba_yt_project.silver
    MANAGED LOCATION 'abfss://anshlamba-yt-project@anshlambaprojectextdl.dfs.core.windows.net/silver';
CREATE SCHEMA IF NOT EXISTS anshlamba_yt_project.gold
    MANAGED LOCATION 'abfss://anshlamba-yt-project@anshlambaprojectextdl.dfs.core.windows.net/gold';


-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Creating Volume Files

-- COMMAND ----------

CREATE EXTERNAL VOLUME IF NOT EXISTS anshlamba_yt_project.landing.files
    LOCATION 'abfss://anshlamba-yt-project@anshlambaprojectextdl.dfs.core.windows.net/landing';
CREATE EXTERNAL VOLUME IF NOT EXISTS anshlamba_yt_project.checkpoints.files
    LOCATION 'abfss://anshlamba-yt-project@anshlambaprojectextdl.dfs.core.windows.net/checkpoints'

-- COMMAND ----------

DROP SCHEMA IF EXISTS anshlamba_yt_project.batch_ingest CASCADE;