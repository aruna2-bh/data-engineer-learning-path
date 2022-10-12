# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC 
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC # Your First Lab
# MAGIC 
# MAGIC Labs are a critical component of our courseware. It's during these labs that students are able to put to practice what they learn.
# MAGIC 
# MAGIC Below are a few key details about labs.
# MAGIC * The number of the lab should correspond to the lesson this lab applies to
# MAGIC * In this example **EC 01 - Getting Started** is the lesson and **EC 01L - Your First Lab** is the lab that corresponds to that lesson
# MAGIC * Lessons should **NOT** include **TODO** & **ANSWER** directives - see [ELC 03 - Build Directives]($./Examples/EC 03 - Build Directives) for more information. These should only be included in labs.
# MAGIC * Labs should align to the lesson's learning objectives, not simply arbitrary code completion for the sake of student engagement in a pointless exercise.
# MAGIC * With 10 notebooks or less, it is OK for lessons and labs to exist in the same folder.
# MAGIC * When there are more than 10 notebooks, it is recomended to relocate labs into a **/Labs** subfolder.

# COMMAND ----------

# MAGIC %run ./Includes/Classroom-Setup-01L

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Present the problem
# MAGIC Lorem ipsum dolor sit amet, consectetur adipiscing elit, sed do eiusmod tempor incididunt ut labore et dolore magna aliqua. Ut enim ad minim veniam, quis nostrud exercitation ullamco laboris nisi ut aliquip ex ea commodo consequat. 
# MAGIC 
# MAGIC Outline the Steps
# MAGIC 1. Duis aute irure dolor in reprehenderit
# MAGIC 1. Sed ut perspiciatis unde omnis iste natus error
# MAGIC 1. Quis autem vel eum iure

# COMMAND ----------

# TODO

# Update this function to return 42 (note my double comment)
def some_function():
  FILL_IN

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Check Your Work
# MAGIC It's not enought to simpley execute their code, give the users, at minimim some asserts that validates their work.
# MAGIC 
# MAGIC Run the follow cell to validate your solution.

# COMMAND ----------

actual = some_function()
assert actual == 42, f"Expected the function some_function() to return 42, found {actual}"

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## What if the test fails?
# MAGIC By design, we expect the following cell to fail. This will demonstrate two things:
# MAGIC * How test failures can be reviewd after a smoke-test failure
# MAGIC * How to configure for expected failures (e.g. unsupported on GCP).
# MAGIC 
# MAGIC To see this configuration, review the notebook **/Build-Scripts/Publish-All** which contains the configuration used by **/Build-Scripts/Test-All-Published**

# COMMAND ----------

assert False, "Ya, this just isn't going to work for us tody"

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Classroom Cleanup
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
