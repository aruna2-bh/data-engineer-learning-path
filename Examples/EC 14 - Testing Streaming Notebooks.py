# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC 
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC # Testing Streaming Notebooks
# MAGIC 
# MAGIC This notebook demonstrates how to author and test streaming notebooks.

# COMMAND ----------

# MAGIC %run ../Includes/Classroom-Setup-14

# COMMAND ----------

customers_path = f"{DA.paths.stream_source}/customers"

customers_query = (spark.readStream
                        .format("cloudFiles")
                        .option("cloudFiles.format", "json")
                        .option("cloudFiles.schemaLocation", f"{DA.paths.working_dir}/customers.schemas")
                        .option("inferScheam", True)
                        .load(customers_path)
                        .writeStream
                        .trigger(processingTime="1 second")
                        .format("delta")
                        .option("checkpointLocation", f"{DA.paths.checkpoints}/customers")
                        .table("customers")
)

# COMMAND ----------

# If we run the assert below, it will fail because the stream
# has yet to be initialized. While asserts are not a common example
# it illustrates the problem of doing *anything* with the stream
# before it has been fully initialized

DA.block_until_stream_is_ready(customers_query)

# COMMAND ----------

expected = spark.read.json(customers_path).count()
total = spark.read.table("customers").count()
assert total == expected, f"Expected {expected} record, found {total}"

customers_query.stop()

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC Continuously running streams are actually difficult to teach to because it is very common for the stream to be exhaused due to interruptions in the class.
# MAGIC 
# MAGIC An alternative is to use **`trigger(once=True)`** or even better **`trigger(availableNow=True)`**

# COMMAND ----------

orders_path = f"{DA.paths.stream_source}/orders"

orders_query = (spark.readStream
                     .format("cloudFiles")
                     .option("cloudFiles.format", "json")
                     .option("cloudFiles.schemaLocation", f"{DA.paths.working_dir}/orders.schemas")
                     .option("inferScheam", True)
                     .load(orders_path)
                     .writeStream
                     .trigger(availableNow=True)
                     .format("delta")
                     .option("checkpointLocation", f"{DA.paths.checkpoints}/orders")
                     .table("orders")
)

# COMMAND ----------

# This time we can use the streaming-native functions to block 
# until we are ready to process the results

orders_query.awaitTermination()

# COMMAND ----------

expected = spark.read.json(orders_path).count()
total = spark.read.table("orders").count()
assert total == expected, f"Expected {expected} record, found {total}"

orders_query.stop()

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2022 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="https://help.databricks.com/">Support</a>
