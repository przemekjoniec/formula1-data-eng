# Databricks notebook source
dbutils.widgets.text("p_batch_id", "")
v_batch_id = dbutils.widgets.get("p_batch_id")

# COMMAND ----------

# MAGIC %run "../00. Common/Config"

# COMMAND ----------

# MAGIC %run "../00. Common/BronzeHelpers"

# COMMAND ----------

source_file = f"{landing_folder_path}/{v_batch_id}/drivers.json"
table_name = f"{catalog_name}.{bronze_schema}.drivers"

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, DateType

name_schema = StructType([
    StructField('givenName', StringType()),
    StructField('familyName', StringType()),
])

drivers_schema = StructType([
    StructField('driverId', StringType()),
    StructField('name', name_schema),
    StructField('dateOfBirth', DateType()),
    StructField('nationality', StringType()),
    StructField('url', StringType())
])

# COMMAND ----------

drivers_df = (
    spark.read
        .format('json')
        .schema(drivers_schema)
        .option('mode', "FAILFAST")
        .load(source_file)
)


# COMMAND ----------

drivers_f_df = add_ingestion_metadata(drivers_df)

# COMMAND ----------

display(drivers_f_df)

# COMMAND ----------

write_to_bronze(
    input_df = drivers_f_df,
    table_name = table_name,
    batch_id = v_batch_id
)

# COMMAND ----------

