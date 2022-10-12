# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC 
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC # My First Lesson
# MAGIC Lorem ipsum dolor sit amet, consectetur adipiscing elit, sed do eiusmod tempor incididunt ut labore et dolore magna aliqua. Ut enim ad minim veniam, quis nostrud exercitation ullamco laboris nisi ut aliquip ex ea commodo consequat.
# MAGIC 
# MAGIC **Learning Objectives**
# MAGIC 1. Sed ut perspiciatis unde omnis iste natus error sit voluptatem accusantium doloremque laudantium
# MAGIC 1. Totam rem aperiam, eaque ipsa quae ab
# MAGIC 1. Illo inventore veritatis et quasi architecto beatae vitae dicta sunt explicabo
# MAGIC 1. Nemo enim ipsam voluptatem quia voluptas

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Classroom Setup
# MAGIC 
# MAGIC > Introduce what the Classroom-Setup script is doing - not really necissary in lessons 2+

# COMMAND ----------

# MAGIC %run ./Includes/Classroom-Setup-01

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Install Datasets
# MAGIC 
# MAGIC Installation of the datasets is no longer an explicit action and instad is handled in the `Classroom-Setup` scripts.
# MAGIC 
# MAGIC Beyond that, the ideal pattern is to install the datasets in the `Workspace-Setup` once for the entire class.

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Other Conventions
# MAGIC 
# MAGIC As you progress through this course, you will see various references to the object **`DA`**. 
# MAGIC 
# MAGIC This object is provided by Databricks Academy and is part of the curriculum and not part of a Spark or Databricsk API.
# MAGIC 
# MAGIC For example, the **`DA`** object exposes useful variables such as your username and various paths to the datasets in this course as seen here bellow

# COMMAND ----------

print(f"Username:          {DA.username}")
print(f"Schema Name:       {DA.schema_name}")
print(f"Working Directory: {DA.paths.working_dir}")
print(f"User DB Location:  {DA.paths.user_db}")

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Talking Point #1
# MAGIC 
# MAGIC * Praesent dignissim condimentum nisi in vulputate
# MAGIC * Suspendisse sed turpis ex
# MAGIC * In id mi in lacus placerat ornare vitae ut nulla
# MAGIC * Mauris id sapien id ligula pretium consequat eget at ipsum
# MAGIC * Mauris feugiat finibus odio, et congue quam rutrum eu
# MAGIC * Ut dui odio, finibus nec mollis facilisis, sollicitudin vel mauris
# MAGIC * Cras id lobortis purus

# COMMAND ----------

# Some code demonstration
print("Hello World")

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC # Talking Point #2
# MAGIC * Ut ac neque arcu.
# MAGIC * Sed scelerisque mi sit amet dapibus varius.
# MAGIC * Etiam tincidunt scelerisque est, id porta metus pulvinar in.

# COMMAND ----------

# Some more code 
print("Hello again")

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Talking Point #3
# MAGIC 
# MAGIC * Etiam non lorem vulputate purus cursus hendrerit at ullamcorper ligula. 
# MAGIC * Fusce neque urna, cursus sit amet mattis et, ultricies ut dolor.
# MAGIC * Nam quis tempor mauris.

# COMMAND ----------

print("Are we having fun?")

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC Vivamus id metus eu arcu molestie maximus nec ac lectus.

# COMMAND ----------

print("We are almost done...")

# COMMAND ----------

print("I promise.")

# COMMAND ----------

print("Just one more thing to do...")

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Clean up Classroom
# MAGIC > Like Classroom-Setup, this needs to be explained only in the first lesson. In lessons 2+ it should be obvious what is going on here.
# MAGIC 
# MAGIC Run the following cell to remove lessons-specific assets created during this lesson:

# COMMAND ----------

DA.cleanup()

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2022 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="https://help.databricks.com/">Support</a>
