# Databricks notebook source
# def test_library_access():
#     import requests

#     sites = [
#         "https://github.com/databricks-academy/dbacademy-gems",
#         "https://github.com/databricks-academy/dbacademy-rest",
#         "https://github.com/databricks-academy/dbacademy-helper",
#         "https://pypi.org/simple/requests",
#         "https://pypi.org/simple/urllib3",
#         "https://pypi.org/simple/overrides",
#         "https://pypi.org/simple/deprecated",
#         "https://pypi.org/simple/wrapt",
#         "https://pypi.org/simple/charset-normalizer",
#         "https://pypi.org/simple/idna",
#     ]
#     for site in sites:
#         response = requests.get(site)
#         assert response.status_code == 200, f"Unable to access GitHub or PyPi resources (HTTP {response.status_code} for {site}). Please see the \"Troubleshooting | Cannot Install Libraries\" section of the \"Version Info\" notebook for more information."
        
# test_library_access()

# COMMAND ----------

# def test_dbfs_writes(dir):
#     import os
#     from contextlib import redirect_stdout
    
#     file = f"{dir}/test.txt"
#     try:
#         with redirect_stdout(None):
#             parent_dir = "/".join(dir.split("/")[:-1])
            
#             if not os.path.exists(parent_dir): os.mkdir(parent_dir)
#             if not os.path.exists(dir): os.mkdir(dir)
#             if os.path.exists(file): os.remove(file)
            
#             with open(file, "w") as f: f.write("Please delete this file")
#             with open(file, "r") as f: f.read()
#             os.remove(file)
            
#     except Exception as e:
#         print(e)
#         raise AssertionError(f"Unable to write to {file}. Please see the \"Troubleshooting | DBFS Writes\" section of the \"Version Info\" notebook for more information.")
            
# test_dbfs_writes("/dbfs/mnt/dbacademy-users")
# test_dbfs_writes("/dbfs/mnt/dbacademy-datasets")

# COMMAND ----------

# MAGIC %pip install \
# MAGIC git+https://github.com/databricks-academy/dbacademy@v1.0.2 \
# MAGIC --quiet --disable-pip-version-check

# COMMAND ----------

# MAGIC %run ./_dataset_index

# COMMAND ----------

from dbacademy import dbgems
from dbacademy.dbhelper import DBAcademyHelper, Paths, CourseConfig, LessonConfig

# The following attributes are externalized to make them easy
# for content developers to update with every new course.

course_config = CourseConfig(course_code = "delp",
                             course_name = "data-engineer-learning-path",
                             data_source_name = "data-engineer-learning-path",
                             data_source_version = "v01",
                             install_min_time = "2 min",
                             install_max_time = "10 min",
                             remote_files = remote_files,
                             supported_dbrs = ["11.3.x-scala2.12", "11.3.x-photon-scala2.12", "11.3.x-cpu-ml-scala2.12"],
                             expected_dbrs = "11.3.x-scala2.12, 11.3.x-photon-scala2.12, 11.3.x-cpu-ml-scala2.12")

# Defined here for the majority of lessons, 
# and later modified on a per-lesson basis.
lesson_config = LessonConfig(name = None,
                             create_schema = True,
                             create_catalog = False,
                             requires_uc = False,
                             installing_datasets = True,
                             enable_streaming_support = False)

# COMMAND ----------

@DBAcademyHelper.monkey_patch
def clone_source_table(self, table_name, source_path, source_name=None):
    start = self.clock_start()

    source_name = table_name if source_name is None else source_name
    print(f"Cloning the \"{table_name}\" table from \"{source_path}/{source_name}\".", end="...")
    
    spark.sql(f"""
        CREATE OR REPLACE TABLE {table_name}
        SHALLOW CLONE delta.`{source_path}/{source_name}`
        """)
    
    print(self.clock_stopped(start))

