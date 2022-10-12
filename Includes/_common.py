# Databricks notebook source
# MAGIC %pip install git+https://github.com/databricks-academy/dbacademy@main --quiet --disable-pip-version-check

# COMMAND ----------

# MAGIC %run ./_dataset_index

# COMMAND ----------

from dbacademy import dbgems
from dbacademy.dbhelper import DBAcademyHelper, Paths, CourseConfig, LessonConfig

# The following attributes are externalized to make them easy
# for content developers to update with every new course.

course_config = CourseConfig(course_code = "exmp",                  # The abbreviated version of the course (4 chars preferred)
                             course_name = "example-course",        # The full name of the course, hyphenated
                             data_source_name = "example-course",   # Should be the same as the course
                             data_source_version = "v01",           # New courses would start with 01
                             install_min_time = "1 min",            # The minimum amount of time to install the datasets (e.g. from Oregon)
                             install_max_time = "5 min",            # The maximum amount of time to install the datasets (e.g. from India)
                             remote_files = remote_files,           # The enumerated list of files in the datasets
                             supported_dbrs = ["11.3.x-scala2.12"])

