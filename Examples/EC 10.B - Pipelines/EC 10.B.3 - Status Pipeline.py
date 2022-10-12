# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC 
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
# MAGIC </div>

# COMMAND ----------

import dlt
import pyspark.sql.functions as F

source = spark.conf.get("source")


@dlt.table
def status_bronze():
    return (
        spark.readStream
            .format("cloudFiles")
            .option("cloudFiles.format", "json")
            .load(f"{source}/status")
            .select(
                F.current_timestamp().alias("processing_time"), 
                F.input_file_name().alias("source_file"), 
                "*"
            )
    )

    
@dlt.table(
        name = "status_silver"
    )
@dlt.expect_or_drop("valid_timestamp", "status_timestamp > 1640995200")
def status_silver():
    return (
        dlt.read_stream("status_bronze")
            .drop("source_file", "_rescued_data")
    )

    
@dlt.table
def email_updates():
    return (
        dlt.read("status_silver").alias("a")
            .join(
                dlt.read("subscribed_order_emails_v").alias("b"), 
                on="order_id"
            ).select(
                "a.*", 
                "b.email"
            )
    )

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2022 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="https://help.databricks.com/">Support</a>
