# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC 
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC # Workspace Setup
# MAGIC This notebook should be run by instructors to prepare the workspace for a class.
# MAGIC 
# MAGIC The key changes this notebook makes includes:
# MAGIC * Updating user-specific grants such that they can create databases/schemas against the current catalog when they are not workspace-admins.
# MAGIC * Configures three cluster policies:
# MAGIC     * **DBAcademy All-Purpose Policy** - which should be used on clusters running standard notebooks.
# MAGIC     * **DBAcademy Jobs-Only Policy** - which should be used on workflows/jobs
# MAGIC     * **DBAcademy DLT-Only Policy** - which should be used on DLT piplines (automatically applied)
# MAGIC * Create or update the shared **Starter Warehouse** for use in Databricks SQL exercises
# MAGIC * Create the Instance Pool **DBAcademy Pool** for use by students and the "student" and "jobs" policies.

# COMMAND ----------

# MAGIC %run ./_common

# COMMAND ----------

import time

# Start a timer so we can benchmark execution duration.
setup_start = int(time.time())

# COMMAND ----------

# MAGIC %md
# MAGIC # Get Class Config
# MAGIC The three variables defined by these widgets are used to configure our environment as a means of controlling class cost.

# COMMAND ----------

# Setup the widgets to collect required parameters.
from dbacademy.dbhelper import WorkspaceHelper # no other option for this course
dbutils.widgets.dropdown("configure_for", WorkspaceHelper.ALL_USERS, [WorkspaceHelper.ALL_USERS], "Configure Workspace For")

# students_count is the reasonable estiamte to the maximum number of students
dbutils.widgets.text("students_count", "", "Number of Students")

# event_name is the name assigned to this event/class or alternatively its class number
dbutils.widgets.text("event_name", "", "Event Name/Class Number")

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC # Init Script & Install Datasets
# MAGIC The main affect of this call is to pre-install the datasets.
# MAGIC 
# MAGIC It has the side effect of create our DA object which includes our REST client.

# COMMAND ----------

lesson_config.create_schema = False                 # We don't need a schema when configuring the workspace

DA = DBAcademyHelper(course_config, lesson_config)
DA.reset_lesson()
DA.init()
DA.conclude_setup()

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Create Class Instance Pools
# MAGIC The following cell configures the instance pool used for this class

# COMMAND ----------

instance_pool_id = DA.workspace.clusters.create_instance_pool()

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Create The Three Class-Specific Cluster Policies
# MAGIC The following cells create the various cluster policies used by the class

# COMMAND ----------

DA.workspace.clusters.create_all_purpose_policy(instance_pool_id)
DA.workspace.clusters.create_jobs_policy(instance_pool_id)
DA.workspace.clusters.create_dlt_policy()
None

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Create Class-Shared Databricks SQL Warehouse/Endpoint
# MAGIC Creates a single wharehouse to be used by all students.
# MAGIC 
# MAGIC The configuration is derived from the number of students specified above.

# COMMAND ----------

DA.workspace.warehouses.create_shared_sql_warehouse(name="Starter Warehouse")

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Configure User Entitlements
# MAGIC 
# MAGIC This task simply adds the "**databricks-sql-access**" entitlement to the "**users**" group ensuring that they can access the Databricks SQL view.

# COMMAND ----------

DA.workspace.add_entitlement_workspace_access()
DA.workspace.add_entitlement_databricks_sql_access()

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Update Grants
# MAGIC This operation executes **`GRANT CREATE ON CATALOG TO users`** to ensure that students can create databases as required by this course when they are not admins.
# MAGIC 
# MAGIC Note: The implementation requires this to execute in another job and as such can take about three minutes to complete.

# COMMAND ----------

# Ensures that all users can create databases on the current catalog 
# for cases wherein the user/student is not an admin.
job_id = DA.workspace.databases.configure_permissions("Configure-Permissions")

# COMMAND ----------

DA.client.jobs().delete_by_id(job_id)

# COMMAND ----------

print(f"Setup completed {DA.clock_stopped(setup_start)}")

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2022 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="https://help.databricks.com/">Support</a>
