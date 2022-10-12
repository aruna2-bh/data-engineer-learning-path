# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC 
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 
# MAGIC # Lab: Orchestrating Jobs with Databricks
# MAGIC 
# MAGIC In this lab, you'll be configuring a multi-task job comprising of:
# MAGIC * A notebook that lands a new batch of data in a storage directory
# MAGIC * A Delta Live Table pipeline that processes this data through a series of tables
# MAGIC * A notebook that queries the gold table produced by this pipeline as well as various metrics output by DLT
# MAGIC 
# MAGIC ## Learning Objectives
# MAGIC By the end of this lab, you should be able to:
# MAGIC * Schedule a notebook as a task in a Databricks Job
# MAGIC * Schedule a DLT pipeline as a task in a Databricks Job
# MAGIC * Configure linear dependencies between tasks using the Databricks Workflows UI

# COMMAND ----------

# MAGIC %run ../../Includes/Classroom-Setup-05.2.1L

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 
# MAGIC ## Land Initial Data
# MAGIC Seed the landing zone with some data before proceeding. 
# MAGIC 
# MAGIC You will re-run this command to land additional data later.

# COMMAND ----------

DA.data_factory.load()

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 
# MAGIC ## Schedule a Notebook Job
# MAGIC 
# MAGIC When using the Jobs UI to orchestrate a workload with multiple tasks, you'll always begin by scheduling a single task.
# MAGIC 
# MAGIC Before we start run the following cell to get the values used in this step.

# COMMAND ----------

DA.print_job_config()

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 
# MAGIC Here, we'll start by scheduling the first notebook.
# MAGIC 
# MAGIC Steps:
# MAGIC 1. Click the **Workflows** button on the sidebar
# MAGIC 1. Select the **Jobs** tab.
# MAGIC 1. Click the blue **Create Job** button
# MAGIC 1. Configure the task:
# MAGIC     1. Enter **Batch-Job** for the task name
# MAGIC     1. For **Type**, select **Notebook**
# MAGIC     1. For **Path**, select the **Batch Notebook Path** value provided in the cell above
# MAGIC     1. From the **Cluster** dropdown, under **Existing All Purpose Clusters**, select your cluster
# MAGIC     1. Click **Create**
# MAGIC 1. In the top-left of the screen, rename the job (not the task) from **`Batch-Job`** (the defaulted value) to the **Job Name** value provided in the cell above.
# MAGIC 1. Click the blue **Run now** button in the top right to start the job.
# MAGIC 
# MAGIC <img src="https://files.training.databricks.com/images/icon_note_24.png"> **Note**: When selecting your all purpose cluster, you will get a warning about how this will be billed as all purpose compute. Production jobs should always be scheduled against new job clusters appropriately sized for the workload, as this is billed at a much lower rate.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Schedule a DLT Pipeline as a Task
# MAGIC 
# MAGIC In this step, we'll add a DLT pipeline to execute after the success of the task we configured at the start of this lesson.
# MAGIC 
# MAGIC So that we can focus on Jobs and not Piplines, we are going to use the following utility command to create the pipeline for us.

# COMMAND ----------

DA.create_pipeline()

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC Steps:
# MAGIC 1. At the top left of your screen, you'll see the **Runs** tab is currently selected; click the **Tasks** tab.
# MAGIC 1. Click the large blue circle with a **+** at the center bottom of the screen to add a new task
# MAGIC 1. Configure the task:
# MAGIC     1. Enter **DLT** for the task name
# MAGIC     1. For **Type**, select  **Delta Live Tables pipeline**
# MAGIC     1. For **Pipeline**, select the pipeline name provided in the cell above<br/>
# MAGIC     1. The **Depends on** field defaults to your previously defined task, **Batch-Job** - leave this value as-is
# MAGIC     1. Click the blue **Create task** button
# MAGIC 
# MAGIC You should now see a screen with 2 boxes and a downward arrow between them. 
# MAGIC 
# MAGIC Your **`Batch-Job`** task will be at the top, leading into your **`DLT`** task. 

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 
# MAGIC ## Schedule an Additional Notebook Task
# MAGIC 
# MAGIC An additional notebook has been provided which queries some of the DLT metrics and the gold table defined in the DLT pipeline. 
# MAGIC 
# MAGIC We'll add this as a final task in our job.
# MAGIC 
# MAGIC Steps:
# MAGIC 1. Click the large blue circle with a **+** at the center bottom of the screen to add a new task
# MAGIC Steps:
# MAGIC 1. Configure the task:
# MAGIC     1. Enter **Query-Results** for the task name
# MAGIC     1. For **Type**, select **Notebook**
# MAGIC     1. For **Path**, select the **Query Notebook Path** value provided at the start of this lesson
# MAGIC     1. From the **Cluster** dropdown, under **Existing All Purpose Clusters**, select your cluster
# MAGIC     1. The **Depends on** field defaults to your previously defined task, **DLT** - leave this value as-is.
# MAGIC     1. Click the blue **Create task** button
# MAGIC     
# MAGIC Click the blue **Run now** button in the top right of the screen to run this job.
# MAGIC 
# MAGIC From the **Runs** tab, you will be able to click on the start time for this run under the **Active runs** section and visually track task progress.
# MAGIC 
# MAGIC Once all your tasks have succeeded, review the contents of each task to confirm expected behavior.

# COMMAND ----------

# ANSWER

# This function is provided for students who do not 
# want to work through the exercise of creating the job.
DA.create_job()

# COMMAND ----------

DA.validate_job_config()

# COMMAND ----------

# ANSWER

# This function is provided to start the job and  
# block until it has completed, canceled or failed
DA.start_job()

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2022 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="https://help.databricks.com/">Support</a>
