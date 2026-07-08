# Databricks notebook source
from pyspark.sql import functions as F

# COMMAND ----------

dbutils.widgets.text("p_batch_id", "")
v_batch_id = dbutils.widgets.get("p_batch_id")

# COMMAND ----------

# MAGIC  %run "../00. Common/Config"
# MAGIC

# COMMAND ----------

# MAGIC %run "../00. Common/SilverHelper"

# COMMAND ----------

bronze_table = f"{catalog_name}.{bronze_schema}.circuits"
silver_table = f"{catalog_name}.{silver_schema}.circuits"


# COMMAND ----------

circuits_df = (
    spark.table(bronze_table).filter((F.col("batch_id") == v_batch_id))
)           

# COMMAND ----------

display(circuits_df)

# COMMAND ----------

circuits_selected_df = circuits_df.select(
    "circuitId",
    "circuitName",
    "lat",
    "long",
    "locality",
    "country",
    "ingestion_timestamp",
    "source_file",
    "batch_id"
)

# COMMAND ----------

circuits_renamed_df = (
    circuits_selected_df
        .withColumnsRenamed({
            "circuitId": "circuit_id",
            "circuitName": "circuit_name",
            "lat": "latitude",
            "long": "longtitude"
        })
)

# COMMAND ----------

circuits_valid_df = circuits_renamed_df.filter("circuit_id IS NOT NULL")

# COMMAND ----------

circuits_distinct_df = circuits_valid_df.dropDuplicates(["circuit_id"])

# COMMAND ----------

display(circuits_distinct_df)

# COMMAND ----------


circuits_final_df = (
    circuits_distinct_df
        .withColumn('circuit_name',F.initcap("circuit_name"))
        .withColumn('locality',F.initcap("locality"))
)

# COMMAND ----------

write_to_silver(
    input_df = circuits_final_df,
    target_table = silver_table,
    merge_condition = "t.circuit_id = s.circuit_id",
    columns_to_update = [
        "circuit_name",
        "latitude",
        "longtitude",
        "locality",
        "country",
        "ingestion_timestamp",
        "source_file",
        "batch_id"
    ]
)

# COMMAND ----------

display(spark.table(silver_table))

# COMMAND ----------

