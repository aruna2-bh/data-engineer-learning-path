# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC 
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC # Testing Multi-Task Jobs with Pipelines
# MAGIC 
# MAGIC This pattern has the following key elements:
# MAGIC * **`DA.get_job_config()`**: This is the common piece of code used by the print and create operations.
# MAGIC * **`DA.print_job_config()`**: Prints user-specific instructions. These instructions are critical to avoiding conflicts with user resources and dealing with requirements like unique names.
# MAGIC * **`DA.create_job()`**: Creates the job for the user. This is the first 1/2 that enables the job to be tested and when put in an **`# ANSWER`** cell, it creates a Solutions notebook that enables users to complete the exercise without actually doing the manual config.
# MAGIC * **`DA.start_job()`**: Runs the job for the user. This is the second 1/2 that enables the job to be tested and when put in an **`# ANSWER`** cell, it creates a Solutions notebook that enables users to complete the exercise without explicitly running it.
# MAGIC 
# MAGIC The definition for each of these can be found in [Classroom-Setup-12]($../Includes/Classroom-Setup-12) and [_multi-task-jobs-with-piplines-config]($../Includes/_multi-task-jobs-with-piplines-config)

# COMMAND ----------

# MAGIC %run ../Includes/Classroom-Setup-12

# COMMAND ----------

DA.print_job_config()

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC The following instructions are generally correct but must be adjusted for your specific needs.
# MAGIC 
# MAGIC Variations include things like Local resources, resources from GitHub, Pipelines (not documented here), cluster configurations (eg, interactive vs 1-per-job and 1-per-task)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Create and Configure a Multi-Task Job
# MAGIC 
# MAGIC ### Create the First Task
# MAGIC 1. Click the **Workflows** button on the sidebar
# MAGIC 1. Select the **Jobs** tab.
# MAGIC 1. Click the **Create Job** button.
# MAGIC 1. In the unlabled title field at the top of the screen (*Add a name for your job...*), set the name of the job to the value specified in the cell above.
# MAGIC 1. In the field **Task name**, enter the value specified in the cell above for **Task #1**
# MAGIC 1. In the field **Type**, select the value **Notebook**
# MAGIC 1. In the field **Source**, select the value **Local**
# MAGIC 1. In the field **Path**, browse to the resource (aka notebook) specified in the cell above for **Task #1**
# MAGIC 1. Configure the **Cluster**:
# MAGIC     1. Expand the cluster selections
# MAGIC     1. In the drop-down, select **New job cluster**
# MAGIC     1. In the field **Cluster name**, keep the default name
# MAGIC     1. In the field **Policy**, select **Unrestricted**
# MAGIC     1. In the field **Cluster mode**, select **Single node**
# MAGIC     1. In the field **Databricks runtime version** select the latest Photon LTS.
# MAGIC     1. In the field **Autopilot options** uncheck **Enable autoscaling local storage**
# MAGIC     1. In the field **Node type**, select the cloud-specific type:
# MAGIC         - AWS: **i3.xlarge**
# MAGIC         - MSA: **Standard_DS3_v2**
# MAGIC         - GCP: **n1-standard-4**
# MAGIC     1. *...insert any other steps specific to your usecase...*
# MAGIC     1. Click **Confirm** to finalize your cluster settings.
# MAGIC 1. *...insert any other steps specific to your usecase...*
# MAGIC 1. Click **Create** to create the first task.
# MAGIC 
# MAGIC ### Create the Second Task
# MAGIC 1. Click the "**+**" button in the bottom center of the screen to add a new task.
# MAGIC 1. In the field **Task name**, enter the value specified in the cell above for **Task #2**
# MAGIC 1. In the field **Type**, select the value **Delta Live Tables pipeline**
# MAGIC 1. In the field **Pipeline**, select the pipline specified in the cell above for **Task #2**
# MAGIC 
# MAGIC ### Create the Thrid Tasks
# MAGIC 1. Click the "**+**" button in the bottom center of the screen to add a new task.
# MAGIC 1. Repeat the steps above for **Create the First Task** substituting the correct configuration as defined in the previous cell.

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC Schedule the job, run it manually, whatever...

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2022 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="https://help.databricks.com/">Support</a>
