# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC 
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC # Installing & Validating Datasets
# MAGIC 
# MAGIC Datasets are maintained in an Azure container and are coppied to the local machine. 
# MAGIC 
# MAGIC This processes is referred to as "installing" and is necissary to avoid the mass replication of previous strategies while simultaneously addressing performance issues that arise when data is in Oregon and the cluster is in Austrailia, for example.
# MAGIC 
# MAGIC However, not all customers can access this container due to security issues - this is very common in the financial (e.g. JP Morgan Chase) and government accounts.
# MAGIC 
# MAGIC To address this, the "install" process is proken down as follows:
# MAGIC 
# MAGIC ## Development
# MAGIC 1. Determine the name of your course: **'example-course'**, for example.
# MAGIC 1. Move your datasets and copyrights to the local workspace in **`dbfs/mnt/dbacademy-datasets/example-course/v01`**, for example.
# MAGIC     * **WARNING** As you develop your course, make sure to treate datasets in this directory as read-only as it is used by all students in the workspace.
# MAGIC 1. Because this is where your datasets will be installed to, you can start development of the course and datasets without worrying about publishing just yet.
# MAGIC 1. Enumerate the local datasets to create a list of files used in validating the install. For this purpose you can call use **`DA.dev.enumerate_local_datasets()`**
# MAGIC 1. Update the enumeration in the **[_common]($../Includes/_common)** notebook, **Cmd 4**.
# MAGIC 
# MAGIC ## Publishing Dataset
# MAGIC 1. Load the data into the Azure data repository.
# MAGIC 1. Enumerate the remote datasets to create a list of files used in validating the install. For this purpose you can call use **`DA.dev.enumerate_remote_datasets()`**
# MAGIC 1. Update the enumeration in the **[_common]($../Includes/_common)** notebook, **Cmd 4**.
# MAGIC 1. Deleting all local files from **`dbfs/mnt/dbacademy-datasets/example-course/v01`**, for example.
# MAGIC 1. Test the install by rerunning **`DA.install_datasets()`**

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Classroom Setup

# COMMAND ----------

# MAGIC %run ../Includes/Classroom-Setup-06

# COMMAND ----------

# MAGIC %md # Enumerate Local Datasets
# MAGIC This next cell simply demonstrates usage of the enumeration utilities for local datasets.

# COMMAND ----------

DA.dev.enumerate_local_datasets()

# COMMAND ----------

# MAGIC %md # Enumerate Remote Datasets
# MAGIC This next cell simply demonstrates usage of the enumeration utilities for local datasets.

# COMMAND ----------

DA.dev.enumerate_remote_datasets()

# COMMAND ----------

# MAGIC %md # Repair on Cleanup
# MAGIC 
# MAGIC Each lesson should be idempotent and as such, we should cleanup all assets **except** the installed datasets.
# MAGIC 
# MAGIC To aid in development and user experience, **`DA.cleanup()`** includes a call to **`DA.validate_datasets()`** and should result in a repair should the local install be corrupted.

# COMMAND ----------

# This line is commented out only to avoid side effects when under test asyncronously.
# Feel free to uncomment it, delete the file, and then repair it in the next cell.
# dbutils.fs.rm(f"{DA.paths.datasets}/sales/customers/00.json")

files = dbutils.fs.ls(f"{DA.paths.datasets}/sales/customers")
display(files)

# COMMAND ----------

DA.cleanup()

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2022 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="https://help.databricks.com/">Support</a>
