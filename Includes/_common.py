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
# MAGIC git+https://github.com/databricks-academy/dbacademy-gems@v1.1.20 \
# MAGIC git+https://github.com/databricks-academy/dbacademy-rest@v1.1.1 \
# MAGIC git+https://github.com/databricks-academy/dbacademy-helper@v2.0.9 \
# MAGIC --quiet --disable-pip-version-check

# COMMAND ----------

# MAGIC %run ./_dataset_index

# COMMAND ----------

from dbacademy_gems import dbgems
from dbacademy_helper import DBAcademyHelper, Paths, CourseConfig, LessonConfig

# The following attributes are externalized to make them easy
# for content developers to update with every new course.

course_config = CourseConfig(course_code = "delp",                              # The abbreviated version of the course
                             course_name = "data-engineer-learning-path",       # The full name of the course, hyphenated
                             data_source_name = "data-engineer-learning-path",  # Should be the same as the course
                             data_source_version = "v01",                       # New courses would start with 01
                             install_min_time = "2 min",                        # The minimum amount of time to install the datasets (e.g. from Oregon)
                             install_max_time = "10 min",                       # The maximum amount of time to install the datasets (e.g. from India)
                             remote_files = remote_files,                       # The enumerated list of files in the datasets
                             supported_dbrs = ["11.3.x-scala2.12"])

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

