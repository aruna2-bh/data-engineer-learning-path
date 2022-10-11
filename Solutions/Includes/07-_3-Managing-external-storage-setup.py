# Databricks notebook source
import re

class DBAcademyHelper():
    def __init__(self):
        import re
        
        # Do not modify this pattern without also updating the Reset notebook.
        username = spark.sql("SELECT current_user()").first()[0]
        clean_username = re.sub("[^a-zA-Z0-9]", "_", username)
        self.catalog = f"dbacademy_{clean_username}"
        self.schema = "external_storage"
        self.storage_credential = f"dbacademy_{clean_username}"
        self.external_location = f"dbacademy_{clean_username}"

        spark.conf.set("da.catalog", self.catalog)
        spark.conf.set("DA.catalog", self.catalog)
        spark.conf.set("da.schema", self.schema)
        spark.conf.set("DA.schema", self.schema)
        spark.conf.set("da.storage_credential", self.storage_credential)
        spark.conf.set("DA.storage_credential", self.storage_credential)
        spark.conf.set("da.external_location", self.external_location)
        spark.conf.set("DA.external_location", self.external_location)
        
        try:
            print(f"\nCreating the catalog \"{self.catalog}\"")
            spark.sql(f"CREATE CATALOG IF NOT EXISTS {self.catalog}")
        except Exception as e:
            # TODO - need to trap the correct exception so that we don't bury other exceptions.
            raise Exception(f"You may not have sufficent permissions (Meta-Store Admin) to create a catalog") from e
                
        print(f"\nCreating the schema \"{self.catalog}.{self.schema}\"")
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS {self.catalog}.{self.schema}")

        spark.sql(f"USE CATALOG {self.catalog}")
        spark.sql(f"USE SCHEMA {self.schema}")
        
    def cleanup(self):
        spark.sql(f"DROP CATALOG IF EXISTS {self.catalog} CASCADE")
        spark.sql(f"DROP EXTERNAL LOCATION IF EXISTS {self.external_location}")
        spark.sql(f"DROP STORAGE CREDENTIAL IF EXISTS {self.storage_credential}")

da = DBAcademyHelper()

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT col1 AS `Object`,col2 AS `Name`
# MAGIC FROM VALUES
# MAGIC   ('Catalog','${da.catalog}'),
# MAGIC   ('Schema','${da.schema}'),
# MAGIC   ('Storage credential','${da.storage_credential}'),
# MAGIC   ('External location','${da.external_location}')
