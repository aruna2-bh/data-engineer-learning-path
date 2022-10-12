# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC 
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC # Build-Time Substitutions
# MAGIC 
# MAGIC Build-time substitutions are placholders in a notebook that are substituted with pre-defined values when the course is published (aka built).
# MAGIC 
# MAGIC You can see in the following cell a large number of examples ranging from the cloud (aka AWS, MSA, GCP), to the Spark Version (aka DBR) to the username of the individual publishing the course.
# MAGIC 
# MAGIC These values are defined with a Python dictionary of key-value pairs and is specified in the **/Build-Scripts/Publish-All** notebook.
# MAGIC 
# MAGIC If a substitution is declared in any notebook but that corresponding value is not defined in the publishing script, the publish operation will fail.

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC 
# MAGIC We are not really doing anything special here short of documenting how to use replacements and showing off dbgems
# MAGIC <pre>
# MAGIC * cloud: AWS
# MAGIC * username: jacob.parr@databricks.com
# MAGIC * spark_version: 11.3.x-scala2.12
# MAGIC * instance_pool_id: 1117-212409-soars13-pool-6plxsi6q
# MAGIC * host_name: cons-webapp-9
# MAGIC * username: jacob.parr@databricks.com
# MAGIC * notebook_path: /Repos/Examples/example-course-source/Build-Scripts/03-Publish-Assets
# MAGIC * notebook_dir: /Repos/Examples/example-course-source/Build-Scripts
# MAGIC * api_endpoint: https://oregon.cloud.databricks.com
# MAGIC </pre>  

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2022 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="https://help.databricks.com/">Support</a>
