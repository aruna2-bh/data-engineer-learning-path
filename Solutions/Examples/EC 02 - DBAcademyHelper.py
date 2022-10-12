# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC 
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC # DBAcademyHelper
# MAGIC 
# MAGIC This notebook demonstrates different features of the provided DBAcademyHelper object

# COMMAND ----------

# MAGIC %run ../Includes/Classroom-Setup-02

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC The "standard" classroom setup uses the **`DBAcademyHelper`** to provide consistency in the setup of each lesson. 
# MAGIC 
# MAGIC Reviewing the cell above, you can see several key points:
# MAGIC * The datasets are installed (or not installed if already present)
# MAGIC * The datasets are validated to ensure they were installed correctly
# MAGIC * May include some misc output for longer, lesson-specific, initialization.
# MAGIC * Advertises all the path variables that students will use in this lesson
# MAGIC * Advertises any tables created on the user's behalf.
# MAGIC * Reseting the environment between lessons and providing new working directories and databases for each lesson.

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC For better obfuscation, readability and teachability, student-facing functions and variables are wrapped in a **`DBAcademyHelper`** object with the instance **`DA`**.

# COMMAND ----------

print(DA)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC You can use various attributes as seen in the following cell.
# MAGIC 
# MAGIC Those attributes attached to the the **`DA.paths`** object are advertised in the call to the Classroom-Setup script as seen above.
# MAGIC 
# MAGIC Attributes and functions attached directly to the **`DA`** object are not advertised, such as **`DA.db_name`** seen in the following cell. However, it should be obvious to all users that it is provided by the helper object. That it is not a Databricks or Spark API, but specific to this courseare and Databricks Academy.

# COMMAND ----------

print(f"DA.db_name:           {DA.db_name}")
print("-"*80)
print(f"DA.paths.user_db:     {DA.paths.user_db}")
print(f"DA.paths.working_dir: {DA.paths.working_dir}")
print(f"DA.paths.magic_tbl:   {DA.paths.magic_tbl}")

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC The **`DA.paths`** values, and some select values attached to the **`DA`** instance (such as **`DA.db_name`**), are also injected into the context making them available in SQL commands as seen in the following cell. You can inject additional values in the method **`DA.conclude_setup()`**.
# MAGIC 
# MAGIC This can be really helpful when we need to reference a dataset in a SQL statement.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC   '${DA.db_name}' as db_name,
# MAGIC   '${DA.paths.working_dir}' as working_dir,
# MAGIC   '${DA.paths.user_db}' as user_db,
# MAGIC   '${DA.paths.magic_tbl}' as magic_tbl

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC A courses should be using the "install" pattern as demonstrated here. This pattern addresses various issues including:
# MAGIC * Standardizing the code providing consistency to instructors and students
# MAGIC * Addressing legal requirments such as advertising copyrights (coming soon)
# MAGIC * Addressing performance issues by "installing" the dataset into the current workspace.
# MAGIC 
# MAGIC Typically, this function is introduced to students in notebook #1 so that they have the option of reinstalling the datasets should they need to.
# MAGIC 
# MAGIC In all other cases, it should be called from every Classroom-Setup script with **`reinstall=False`** so that the dataset is installed even if they skip lesson #1, but doesn't reinstall if the dataset already exists.
# MAGIC 
# MAGIC In addition to this, the call to this function ensures that the datasets are installed correctly by ensuring the proper count of all files locally and in the azure data repositry.

# COMMAND ----------

DA.install_datasets(reinstall_datasets=False)

# COMMAND ----------

# MAGIC %md So as to minimize the number of calls in the Classroom-Setup script, `DA.install_datasets()` is called automatically from `DA.init()` as seen here with its companion argument `create_db`:

# COMMAND ----------

DA.init(install_datasets=True, create_db=False)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC With the datasets installed, we can start querying the data on disk.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM delta.`${DA.paths.magic_tbl}`;

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC If you note the output of the Classroom-Setup script, we did have one table created for us which we can execute queries against.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM magic

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC The last command of most every sell is the **`cleanup()`** command that:
# MAGIC * Stops all running streams
# MAGIC * Drop any databases created in this lesson
# MAGIC * Remove from DBFS, specifically from the user's working directory, any on-disk assets created in this lesson.
# MAGIC * Validates the locally installed, class-shared, datasets and attempts to repair them if necissary

# COMMAND ----------

# MAGIC %md 
# MAGIC Run the following cell to delete the tables and files associated with this lesson.

# COMMAND ----------

DA.cleanup()

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2022 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="https://help.databricks.com/">Support</a>
