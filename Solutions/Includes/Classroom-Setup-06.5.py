# Databricks notebook source
# MAGIC %run ./_common

# COMMAND ----------

lesson_config = LessonConfig(name = None,
                             create_schema = False,
                             create_catalog = True,
                             requires_uc = True,
                             installing_datasets = True,
                             enable_streaming_support = False)

DA = DBAcademyHelper(course_config=course_config,
                     lesson_config=lesson_config)
DA.reset_lesson()

DA.init()

DA.conclude_setup()

# COMMAND ----------

DA.other_schema_name = f"{DA.lesson_config.initial_catalog}.{DA.to_schema_name(username=DA.username)}"

spark.sql(f"DROP DATABASE IF EXISTS {DA.other_schema_name} CASCADE")
spark.sql(f"CREATE DATABASE {DA.other_schema_name}")
(spark.read
      .option("header", True)
      .csv(f"{DA.paths.datasets}/movie_ratings/movies.csv")
      .write
      .mode("overwrite")
      .saveAsTable(f"{DA.other_schema_name}.movies"))

print(f"Predefined tables in \"{DA.other_schema_name}\":")
tables = spark.sql(f"SHOW TABLES IN {DA.other_schema_name}").filter("isTemporary == false").select("tableName").collect()
if len(tables) == 0: print("  -none-")
for row in tables: print(f"  {row[0]}")


