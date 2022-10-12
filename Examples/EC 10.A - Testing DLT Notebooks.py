# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC 
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC # Testing DLT Notebooks
# MAGIC 
# MAGIC This pattern has the following key elements:
# MAGIC * **`DA.get_pipeline_config()`**: This is the common piece of code used by the print and create operations.
# MAGIC * **`DA.print_pipeline_config()`**: Prints user-specific instructions. These instructions are critical to avoiding conflicts with user resources and dealing with requirements like unique names.
# MAGIC * **`DA.create_pipeline()`**: Creates the pipline for the user. This is the first 1/2 that enables the pipline to be tested and when put in an **`# ANSWER`** cell, it creates a Solutions notebook that enables users to complete the exercise without actually doing the manual config.
# MAGIC * **`DA.start_pipeline()`**: Starts the pipline for the user. This is the second 1/2 that enables the pipline to be tested and when put in an **`# ANSWER`** cell, it creates a Solutions notebook that enables users to complete the exercise without explicitly starting it.
# MAGIC 
# MAGIC The definition for each of these can be found in [Classroom-Setup-10]($../Includes/Classroom-Setup-10) and [_pipeline_config]($../Includes/_pipeline_config)

# COMMAND ----------

# MAGIC %run ../Includes/Classroom-Setup-10

# COMMAND ----------

DA.print_pipeline_config()

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC The following instructions are generally correct but must be adjusted for your specific needs

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Create and Configure a Pipeline
# MAGIC 
# MAGIC 1. Click the **Jobs** button on the sidebar
# MAGIC 1. Select the **Delta Live Tables** tab.
# MAGIC 1. Click the **Create Pipeline** button.
# MAGIC 1. In the field **Product edition**, select the value "**Advanced**".
# MAGIC 1. In the field **Pipeline Name**, enter the value specified in the cell above
# MAGIC 1. In the field **Notebook Libraries**
# MAGIC     1. Copy the path for notebook #1 specified in the cell above and paste it here.
# MAGIC     2. Click the **Add notebook library** button
# MAGIC     3. Copy the path for notebook #2 specified in the cell above and paste it into the second field.
# MAGIC     4. Click the **Add notebook library** button
# MAGIC     5. Copy the path for notebook #3 specified in the cell above and paste it into the third field.
# MAGIC 1. Configure the Source
# MAGIC     1. Click the **`Add configuration`** button
# MAGIC     1. In the field **Key**, enter the word "**source**"
# MAGIC     1. In the field **Value**, enter the **Source** value specified in the cell above
# MAGIC 1. In the field **Target**, enter the value specified in the cell above.
# MAGIC 1. In the field **Stroage location**, enter the value specified in the cell above.
# MAGIC 1. Set **Pipeline Mode** to **Triggered**.
# MAGIC 1. Disable autoscaling by unchecking **Enable autoscaling**.
# MAGIC 1. In the fields **Workers**, set the value to "**1**" (one)
# MAGIC 1. Click the **Create** button
# MAGIC 1. Verify that the pipeline mode is set to "**Development**"

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC Review the pipeline.. bla bla bla
# MAGIC 
# MAGIC Then load more data into the pipeline

# COMMAND ----------

DA.dlt_data_factory.load()

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC Review the pipeline again.. bla bla bla
# MAGIC 
# MAGIC Load all the data...

# COMMAND ----------

# TODO
This should run for a little over 5 minutes. To abort
early, click the Stop Execution button above
DA.dlt_data_factory.load(continuous=True, delay_seconds=10)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC Review the pipeline again

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2022 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="https://help.databricks.com/">Support</a>
