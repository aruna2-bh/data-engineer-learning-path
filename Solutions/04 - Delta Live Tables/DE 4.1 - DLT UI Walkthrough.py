# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC 
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC # Using the Delta Live Tables UI
# MAGIC 
# MAGIC This demo will explore the DLT UI. By the end of this lesson you will be able to: 
# MAGIC 
# MAGIC * Deploy a DLT pipeline
# MAGIC * Explore the resultant DAG
# MAGIC * Execute an update of the pipeline

# COMMAND ----------

# MAGIC %md
# MAGIC ## Classroom Setup
# MAGIC 
# MAGIC Run the following cell to configure your working environment for this course.

# COMMAND ----------

# MAGIC %run ../Includes/Classroom-Setup-04.1

# COMMAND ----------

# MAGIC %md ## Generate Pipeline Configuration
# MAGIC The configuration of your pipeline includes parameters unique to a given user.
# MAGIC 
# MAGIC You will need to specify which language to use by uncommenting the appropriate line.
# MAGIC 
# MAGIC Run the following cell to print out the values used to configure your pipeline in subsequent steps.

# COMMAND ----------

pipeline_language = "SQL"
# pipeline_language = "Python"

DA.print_pipeline_config(pipeline_language)

# COMMAND ----------

# MAGIC %md <img src="https://files.training.databricks.com/images/icon_hint_24.png"> **HINT:** You will want to refer back to the paths above for Notebook #2 and Notebook #3 in later lessons.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create and configure a pipeline
# MAGIC 
# MAGIC In this section you will create a pipeline using a single notebook provided with the courseware.
# MAGIC 
# MAGIC We'll explore the contents of this notebook in the following lesson.
# MAGIC 
# MAGIC We will later add Notebooks #2 & #3 to the pipeline but for now, let's focus on just Notebook #1:
# MAGIC 
# MAGIC 1. Click the **Workflows** button in the sidebar
# MAGIC 1. Select the **Delta Live Tables** tab.
# MAGIC 1. Click the **Create Pipeline** button.
# MAGIC 1. In the field **Product edition**, select the value "**Advanced**".
# MAGIC 1. In the field **Pipeline Name**, enter the value specified in the cell above
# MAGIC 1. In the field **Notebook Libraries**, copy the path for Notebook #1 specified in the cell above and paste it here.
# MAGIC     * Though this document is a standard Databricks Notebook, the syntax is specialized to DLT table declarations.
# MAGIC     * We will be exploring the syntax in the exercise that follows.
# MAGIC     * Notebooks #2 and #3 will be added in later lessons
# MAGIC 1. Configure the Source
# MAGIC     1. Click the **Add configuration** button
# MAGIC     1. In the field **Key**, enter the word "**source**"
# MAGIC     1. In the field **Value**, enter the **Source** value specified in the cell above
# MAGIC 1. In the field **Storage location**, enter the value specified in the cell above.
# MAGIC     * This optional field allows the user to specify a location to store logs, tables, and other information related to pipeline execution. If not specified, DLT will automatically generate a directory.
# MAGIC 1. Set **Pipeline Mode** to **Triggered**.
# MAGIC     * This field specifies how the pipeline will be run.
# MAGIC     * **Triggered** pipelines run once and then shut down until the next manual or scheduled update.
# MAGIC     * **Continuous** pipelines run continuously, ingesting new data as it arrives.
# MAGIC     * Choose the mode based on latency and cost requirements.
# MAGIC 1. Disable autoscaling by unchecking **Enable autoscaling**.
# MAGIC     * **Enable autoscaling**, **Min Workers** and **Max Workers** control the worker configuration for the underlying cluster processing the pipeline.
# MAGIC     * Notice the DBU estimate provided, similar to that provided when configuring interactive clusters.

# COMMAND ----------

# MAGIC %md
# MAGIC Running in Local-Mode    
# MAGIC * We need to configure the pipeline to run on a local-mode cluster.
# MAGIC * In most deployments, we would adjust the number of workers to accomodate the scale of the pipeline.
# MAGIC * Our datasets are really small, we are just prototyping, so we can use a Local-Mode cluster which reduces cloud costs by employing only a single VM.
# MAGIC 
# MAGIC Local-Mode Setup:    
# MAGIC 1. In the fields **Workers**, set the value to "**0**" (zero) - worker and driver will use the same VM.
# MAGIC 1. Click **Add configuration** and then set the key to **spark.master** and the corresponding value to **local[*]**
# MAGIC 
# MAGIC <img src="https://files.training.databricks.com/images/icon_note_24.png"> **WARNING:** Setting works to zero and failing to configure **spark.master** will result in a failure to create your cluster as it will be waiting forever for a worker that will never be created.

# COMMAND ----------

# MAGIC %md
# MAGIC Final Steps
# MAGIC 1. Click the **Create** button
# MAGIC 1. Verify that the pipeline mode is set to "**Development**"

# COMMAND ----------

# ANSWER

# This function is provided for students who do not 
# want to work through the exercise of creating the pipeline.
DA.create_pipeline(pipeline_language)

# COMMAND ----------

DA.validate_pipeline_config(pipeline_language)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Run a pipeline
# MAGIC 
# MAGIC With a pipeline created, you will now run the pipeline.
# MAGIC 
# MAGIC 1. Select **Development** to run the pipeline in development mode. Development mode provides for more expeditious iterative development by reusing the cluster (as opposed to creating a new cluster for each run) and disabling retries so that you can readily identify and fix errors. Refer to the <a href="https://docs.databricks.com/data-engineering/delta-live-tables/delta-live-tables-user-guide.html#optimize-execution" target="_blank">documentation</a> for more information on this feature.
# MAGIC 2. Click **Start**.
# MAGIC 
# MAGIC The initial run will take several minutes while a cluster is provisioned. Subsequent runs will be appreciably quicker.

# COMMAND ----------

# ANSWER

# This function is provided to start the pipeline and  
# block until it has completed, canceled or failed
DA.start_pipeline()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exploring the DAG
# MAGIC 
# MAGIC As the pipeline completes, the execution flow is graphed. 
# MAGIC 
# MAGIC Selecting the tables reviews the details.
# MAGIC 
# MAGIC Select **orders_silver**. Notice the results reported in the **Data Quality** section. 
# MAGIC 
# MAGIC With each triggered update, all newly arriving data will be processed through your pipeline. Metrics will always be reported for current run.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Land another batch of data
# MAGIC 
# MAGIC Run the cell below to land more data in the source directory, then manually trigger a pipeline update.

# COMMAND ----------

DA.dlt_data_factory.load()

# COMMAND ----------

# MAGIC %md
# MAGIC As we continue through the course, you can return to this notebook and use the method provided above to land new data.
# MAGIC 
# MAGIC Running this entire notebook again will delete the underlying data files for both the source data and your DLT Pipeline. 
# MAGIC 
# MAGIC If you get disconnected from your cluster or have some other event where you wish to land more data without deleting things, refer to the <a href="$./DE 4.99 - Land New Data" target="_blank">DE 4.99 - Land New Data</a> notebook.

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2022 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="https://help.databricks.com/">Support</a>
