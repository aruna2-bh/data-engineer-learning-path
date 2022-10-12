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
# MAGIC # Orchestrating Jobs with Databricks Workflows
# MAGIC 
# MAGIC New updates to the Databricks Jobs UI have added the ability to schedule multiple tasks as part of a job, allowing Databricks Jobs to fully handle orchestration for most production workloads.
# MAGIC 
# MAGIC Here, we'll start by reviewing the steps for scheduling a notebook task as a triggered standalone job, and then add a dependent task using a DLT pipeline. 
# MAGIC 
# MAGIC ## Learning Objectives
# MAGIC By the end of this lesson, you should be able to:
# MAGIC * Schedule a notebook task in a Databricks Workflow Job
# MAGIC * Describe job scheduling options and differences between cluster types
# MAGIC * Review Job Runs to track progress and see results
# MAGIC * Schedule a DLT pipeline task in a Databricks Workflow Job
# MAGIC * Configure linear dependencies between tasks using the Databricks Workflows UI

# COMMAND ----------

# MAGIC %run ../../Includes/Classroom-Setup-05.1.1

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC ## Schedule a Notebook Job
# MAGIC 
# MAGIC When using the Jobs UI to orchestrate a workload with multiple tasks, you'll always begin by scheduling a single task.
# MAGIC 
# MAGIC Before we start run the following cell to get the values used in this step.

# COMMAND ----------

DA.print_job_config_v1()

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC Here, we'll start by scheduling the next notebook.
# MAGIC 
# MAGIC Steps:
# MAGIC 1. Click the **Workflows** button on the sidebar.
# MAGIC 1. Select the **Jobs** tab.
# MAGIC 1. Click the **Create Job** button.
# MAGIC 1. Configure the task:
# MAGIC     1. Enter **Reset** for the task name
# MAGIC     1. For **Type**, select **Notebook**
# MAGIC     1. For **Path**, select the **Reset Notebook Path** value provided in the cell above
# MAGIC     1. From the **Cluster** dropdown, under **Existing All Purpose Clusters**, select your cluster
# MAGIC     1. Click **Create**
# MAGIC 1. In the top-left of the screen, rename the job (not the task) from **`Reset`** (the defaulted value) to the **Job Name** value provided in the cell above.
# MAGIC 1. Click the blue **Run now** button in the top right to start the job.
# MAGIC 
# MAGIC <img src="https://files.training.databricks.com/images/icon_note_24.png"> **Note**: When selecting your all-purpose cluster, you will get a warning about how this will be billed as all-purpose compute. Production jobs should always be scheduled against new job clusters appropriately sized for the workload, as this is billed at a much lower rate.

# COMMAND ----------

# ANSWER

# This function is provided for students who do not 
# want to work through the exercise of creating the job.
DA.create_job_v1()

# COMMAND ----------

DA.validate_job_v1_config()

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 
# MAGIC ## Cron Scheduling of Databricks Jobs
# MAGIC 
# MAGIC Note that on the right hand side of the Jobs UI, directly under the **Job Details** section is a section labeled **Schedule**.
# MAGIC 
# MAGIC Click on the **Edit schedule** button to explore scheduling options.
# MAGIC 
# MAGIC Changing the **Schedule type** field from **Manual** to **Scheduled** will bring up a cron scheduling UI.
# MAGIC 
# MAGIC This UI provides extensive options for setting up chronological scheduling of your Jobs. Settings configured with the UI can also be output in cron syntax, which can be edited if custom configuration not available with the UI is needed.
# MAGIC 
# MAGIC At this time, we'll leave our job set to the **Manual (Paused)** scheduling type.

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 
# MAGIC ## Review Run
# MAGIC 
# MAGIC As currently configured, our single notebook provides identical performance to the legacy Databricks Jobs UI, which only allowed a single notebook to be scheduled.
# MAGIC 
# MAGIC To Review the Job Run
# MAGIC 1. Select the **Runs** tab in the top-left of the screen (you should currently be on the **Tasks** tab)
# MAGIC 1. Find your job. If **the job is still running**, it will be under the **Active runs** section. If **the job finished running**, it will be under the **Completed runs** section
# MAGIC 1. Open the Output details by clicking on the timestamp field under the **Start time** column
# MAGIC 1. If **the job is still running**, you will see the active state of the notebook with a **Status** of **`Pending`** or **`Running`** in the right side panel. If **the job has completed**, you will see the full execution of the notebook with a **Status** of **`Succeeded`** or **`Failed`** in the right side panel
# MAGIC   
# MAGIC The notebook employs the magic command **`%run`** to call an additional notebook using a relative path. Note that while not covered in this course, <a href="https://docs.databricks.com/repos.html#work-with-non-notebook-files-in-a-databricks-repo" target="_blank">new functionality added to Databricks Repos allows loading Python modules using relative paths</a>.
# MAGIC 
# MAGIC The actual outcome of the scheduled notebook is to reset the environment for our new job and pipeline.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Schedule a DLT Pipeline as a Task
# MAGIC 
# MAGIC The pipeline we create here is a simplified version of the one in the previous unit.
# MAGIC 
# MAGIC We will use it as part of a scheduled job in this step.
# MAGIC 
# MAGIC So that we can focus on Jobs and not Piplines, we are going to use the following utility command to create the pipline for us.

# COMMAND ----------

DA.create_pipeline()

# COMMAND ----------

# MAGIC %md
# MAGIC Next, we need to add the task to run this pipeline.
# MAGIC 
# MAGIC 
# MAGIC Steps:
# MAGIC 1. At the top left of your screen, you'll see the **Runs** tab is currently selected; click the **Tasks** tab.
# MAGIC 1. Click the large blue circle with a **+** at the center bottom of the screen to add a new task
# MAGIC 1. Configure the task:
# MAGIC     1. Enter **DLT** for the task name
# MAGIC     1. For **Type**, select  **Delta Live Tables pipeline**
# MAGIC     1. For **Pipeline**, select the DLT pipeline you configured previously in this exercise<br/>
# MAGIC     Note: The pipeline will start with **DLT-Job-Demo-61** and will end with your email address.
# MAGIC     1. The **Depends on** field defaults to your previously defined task, **Reset** - leave this value as-is.
# MAGIC     1. Click the blue **Create task** button
# MAGIC 
# MAGIC You should now see a screen with 2 boxes and a downward arrow between them. 
# MAGIC 
# MAGIC Your **`Reset`** task will be at the top, leading into your **`DLT`** task. 
# MAGIC 
# MAGIC This visualization represents the dependencies between these tasks.
# MAGIC 
# MAGIC Click **Run now** to execute your job.
# MAGIC 
# MAGIC **NOTE**: You may need to wait a few minutes as infrastructure for your job and pipeline is deployed.

# COMMAND ----------

# ANSWER

# This function is provided for students who do not 
# want to work through the exercise of creating the job.
DA.create_job_v2()

# COMMAND ----------

DA.validate_job_v2_config()

# COMMAND ----------

# ANSWER

# This function is provided to start the pipeline and  
# block until it has completed, canceled or failed
DA.start_job()

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 
# MAGIC ## Review Multi-Task Run Results
# MAGIC 
# MAGIC Select the **Runs** tab again and then the most recent run under **Active runs** or **Completed runs** depending on if the job has completed or not.
# MAGIC 
# MAGIC The visualizations for tasks will update in real time to reflect which tasks are actively running, and will change colors if task failures occur. 
# MAGIC 
# MAGIC Clicking on a task box will render the scheduled notebook in the UI. 
# MAGIC 
# MAGIC You can think of this as just an additional layer of orchestration on top of the previous Databricks Jobs UI, if that helps; note that if you have workloads scheduling jobs with the CLI or REST API, <a href="https://docs.databricks.com/dev-tools/api/latest/jobs.html" target="_blank">the JSON structure used to configure and get results about jobs has seen similar updates to the UI</a>.
# MAGIC 
# MAGIC **NOTE**: At this time, DLT pipelines scheduled as tasks do not directly render results in the Runs GUI; instead, you will be directed back to the DLT Pipeline GUI for the scheduled Pipeline.

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2022 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="https://help.databricks.com/">Support</a>
