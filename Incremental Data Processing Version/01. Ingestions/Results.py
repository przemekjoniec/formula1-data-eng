# Databricks notebook source
dbutils.widgets.text("p_batch_id", "")
v_batch_id = dbutils.widgets.get("p_batch_id")

# COMMAND ----------

# MAGIC %run "../00. Common/Config"

# COMMAND ----------

# MAGIC %run "../00. Common/BronzeHelpers"

# COMMAND ----------

source_file = f"{landing_folder_path}/{v_batch_id}/results"
table_name = f"{catalog_name}.{bronze_schema}.results"

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType, FloatType

results_schema = StructType([
    StructField("date", DateType()),
    StructField("raceName", StringType()),
    StructField("round", IntegerType()),
    StructField("season", IntegerType()),
    StructField("url", StringType()),
    StructField("constructorId", StringType()),
    StructField("driverId", StringType()),
    StructField("grid", IntegerType()),
    StructField("laps", IntegerType()),
    StructField("number", IntegerType()),
    StructField("points", FloatType()),
    StructField("position", IntegerType()),
    StructField("positionText", StringType()),
    StructField("status", StringType())
])



# COMMAND ----------

results_df = (
    spark.read
        .format('json')
        .schema(results_schema)
        .option('mode', "FAILFAST")
        .load(source_file)
)


# COMMAND ----------

results_f_df = add_ingestion_metadata(results_df)

# COMMAND ----------

display(results_f_df)

# COMMAND ----------


write_to_bronze(
    input_df = results_f_df,
    table_name = table_name,
    batch_id = v_batch_id
)

# COMMAND ----------

