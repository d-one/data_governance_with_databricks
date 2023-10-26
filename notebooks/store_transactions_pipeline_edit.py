# Databricks notebook source
# MAGIC %md
# MAGIC #### Setup

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS store_data_catalog.blog_post_store_transactions_db_sca;
# MAGIC
# MAGIC USE store_data_catalog.blog_post_store_transactions_db_sca

# COMMAND ----------

import pyspark.sql.functions as F
import pandas as pd
import numpy as np
import time as ts

# COMMAND ----------

# MAGIC %md
# MAGIC ###Loading store transactions
# MAGIC Available in repo ./data.

# COMMAND ----------

# data location in repo
path = "../data/store_transactions_data.csv"

# COMMAND ----------

# empty fields can be loaded as NaN which confuses spark
transactions_raw_pd = pd.read_csv(path).replace({np.nan: None})
# normalise the titles, and remove spaces
transactions_raw_pd.columns = transactions_raw_pd.columns.str.lower()
transactions_raw_pd.columns = transactions_raw_pd.columns.str.title()
transactions_raw_pd.columns = transactions_raw_pd.columns.str.replace(" ","")

# overwrite schema allows for a completely new table to be entered - remove when not needed
(
    spark
    .createDataFrame(transactions_raw_pd)
    .write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("transactions_raw")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Pre-processing raw data

# COMMAND ----------

# read delta table
transactions_raw_sp = spark.read.table("transactions_raw")

# cast to timestamp where appropriate
transactions_raw_sp = transactions_raw_sp.withColumn("Time", F.to_timestamp("Time"))

# the minimal information we need is order id
transactions_clean = transactions_raw_sp.where(F.col("OrderId").isNotNull())

# handling Excel calculations gone wrong
transactions_clean = transactions_clean.na.replace({'#NUM!': None})

# margin is a string percentage, we convert it to a float
transactions_clean = transactions_clean.withColumn("Margin", F.regexp_replace("Margin", "%", ""))
transactions_clean = transactions_clean.withColumn("Margin", F.col("Margin").cast("double")/100)

# COMMAND ----------

# write clean silver table
(
    transactions_clean
    .write
    .mode("overwrite")
    .option("mergeSchema", "true")
    .saveAsTable("transactions_clean")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ###Product profitability analysis

# COMMAND ----------

# MAGIC %sql
# MAGIC -- extracting the needed columns and removing duplicates (caused by multiple registrations due to different order statuses)
# MAGIC CREATE OR REPLACE TEMP VIEW product_profit_analysis_pre AS
# MAGIC SELECT DISTINCT
# MAGIC   OrderId,
# MAGIC   ProductId,
# MAGIC   Brand,
# MAGIC   ProductCategory,
# MAGIC   Quantity,
# MAGIC   Margin,
# MAGIC   SellPrice
# MAGIC FROM transactions_clean

# COMMAND ----------

# MAGIC %sql
# MAGIC -- aggregating for summary statistics
# MAGIC CREATE OR REPLACE TABLE product_profit_analysis AS
# MAGIC SELECT 
# MAGIC   *,
# MAGIC   TotalPurchases*SellPrice*Margin AS Profit
# MAGIC FROM (
# MAGIC   SELECT 
# MAGIC     ProductId,
# MAGIC     Brand,
# MAGIC     ProductCategory,
# MAGIC     SUM(Quantity) AS TotalPurchases,
# MAGIC     AVG(SellPrice) AS SellPrice,
# MAGIC     AVG(Margin) AS Margin
# MAGIC   FROM transactions_clean
# MAGIC   GROUP BY
# MAGIC     ProductId,
# MAGIC     Brand,
# MAGIC     ProductCategory
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC --documenting 
# MAGIC COMMENT ON TABLE product_profit_analysis IS "Summary statistics for product profitability analysis based on all store transactions.";
# MAGIC ALTER TABLE product_profit_analysis CHANGE ProductId ProductId STRING COMMENT "The product ID is not a guaranteed unique identifier.";
# MAGIC ALTER TABLE product_profit_analysis CHANGE TotalPurchases TotalPurchases BIGINT COMMENT "Total number of products sold.";
# MAGIC ALTER TABLE product_profit_analysis CHANGE SellPrice SellPrice DOUBLE COMMENT "Average sell price of the product [CHF].";
# MAGIC ALTER TABLE product_profit_analysis CHANGE Margin Margin DOUBLE COMMENT "Average margin between sale and purchase price [%].";
# MAGIC ALTER TABLE product_profit_analysis CHANGE Profit Profit DOUBLE COMMENT "Total profit [CHF].";

# COMMAND ----------

# MAGIC %md
# MAGIC ###Data quality checks
# MAGIC Log the status of the raw data

# COMMAND ----------

# To register with all the health checks
health_check_ts = pd.Timestamp.now()
print("Current timestamp to be used for dq:", health_check_ts)

# data health checks require a Pandas dataframe
transactions_raw_pd = transactions_raw_sp.toPandas()

# COMMAND ----------

dq_null_res = pd.DataFrame({
    "name": "Null check", 
    "check_type": "clean",
    "data_element": "Column: OrderId",
    "description": "Calculates the portion of null values.",
    "val": transactions_raw_pd["OrderId"].isnull().sum() / transactions_raw_pd.shape[0],
    "max_val": 0,
    "min_val": 0,
    "info": ""
}, index=[0])

latest_val = transactions_raw_pd["Time"].max()
dq_latest_entry_res = pd.DataFrame({
    "name": "Latest entry", 
    "check_type": "current",
    "data_element": "Column: Time",
    "description": "The difference in hours between the latest entry and current timestamp.",
    "val": (health_check_ts - latest_val)/pd.Timedelta(hours=1),
    "max_val": 2,
    "min_val": 1,
    "info": "Test timestamp: "+ts.strftime('%Y-%m-%d %X')+". Last entry on: "+latest_val.strftime('%Y-%m-%d %X')
}, index=[0])

dq_total = pd.concat([
    dq_null_res,
    dq_latest_entry_res
])
dq_total["timestamp"] = health_check_ts

# COMMAND ----------

# write the dq results table to schema
(
    spark
    .createDataFrame(dq_total)
    .write
    .mode("overwrite")
    .option("mergeSchema", "true")
    .saveAsTable("transactions_raw_dq") 
)
