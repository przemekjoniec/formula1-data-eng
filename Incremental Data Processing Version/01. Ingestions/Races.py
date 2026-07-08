# Databricks notebook source
dbutils.widgets.text("p_batch_id", "")
v_batch_id = dbutils.widgets.get("p_batch_id")

# COMMAND ----------

# MAGIC %run "../00. Common/Config"

# COMMAND ----------

# MAGIC %run "../00. Common/BronzeHelpers"

# COMMAND ----------

source_file = f"{landing_folder_path}/{v_batch_id}/races.csv"
table_name = f"{catalog_name}.{bronze_schema}.races"

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, DateType

races_schema = StructType([
    StructField('season', IntegerType()),
    StructField('round', IntegerType()),
    StructField('url', StringType()),
    StructField('raceName', StringType()),
    StructField('date', DateType()),
    StructField('circuitID', StringType())
])


# COMMAND ----------

races_df = (
    spark.read
    .format('csv')
    .option('header','true')
   # .option('inferSchema','true')
    .option('mode','FAILFAST')
    .schema(races_schema)
    .load('/Volumes/formula1/landing/files/landing/races.csv')
)


# COMMAND ----------

races_f_df = add_ingestion_metadata(races_df)

# COMMAND ----------

display(races_f_df)

# COMMAND ----------

write_to_bronze(
    input_df = races_f_df,
    table_name = table_name,
    batch_id = v_batch_id
)

# COMMAND ----------

display(spark.table(table_name))