# Databricks notebook source
# MAGIC %run ./_common

# COMMAND ----------

lesson_config = LessonConfig(name = None,                      # The name of the course - used to cary state between notebooks
                             create_schema = True,             # True if the user-specific schama (database) should be created
                             create_catalog = False,           # Requires UC, but when True creates the user-specific catalog
                             requires_uc = False,              # Indicates if this course requires UC or not
                             installing_datasets = True,       # Indicates that the datasets should be installed or not
                             enable_streaming_support = False) # Indicates that this lesson uses streaming (e.g. needs a checkpoint directory)

DA = DBAcademyHelper(course_config=course_config,              # Create the DA object
                     lesson_config=lesson_config)
DA.reset_lesson()                                              # Reset the lesson to a clean state
DA.init()                                                      # Performs basic intialization including creating schemas and catalogs
DA.conclude_setup()                                            # Finalizes the state and prints the config for the student

