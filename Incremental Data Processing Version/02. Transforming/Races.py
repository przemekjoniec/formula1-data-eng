# Databricks notebook source
dbutils.widgets.text("p_batch_id", "")
v_batch_id = dbutils.widgets.get("p_batch_id")

# COMMAND ----------

# MAGIC %run "../00. Common/Config"

# COMMAND ----------

# MAGIC %run "../00. Common/SilverHelper"

# COMMAND ----------

bronze_table = f"{catalog_name}.{bronze_schema}.races"
silver_table = f"{catalog_name}.{silver_schema}.races"


# COMMAND ----------

races_df = spark.table(bronze_table).filter((F.col("batch_id") == v_batch_id))

# COMMAND ----------

display(races_df)

# COMMAND ----------

races_selected_df = races_df.drop("url")

# COMMAND ----------

races_renamed_df = (
    races_selected_df
        .withColumnsRenamed({
            "circuitId": "circuit_id",
            "raceName": "race_name",
        })
)

# COMMAND ----------

races_distinct_df = races_renamed_df.dropDuplicates(["season","round"])

# COMMAND ----------

display(races_distinct_df)

# COMMAND ----------

import pyspark.sql.functions as F
races_final_df = (
    races_distinct_df
        .withColumn('race_name',F.initcap("race_name"))
)

# COMMAND ----------

display(races_final_df)

# COMMAND ----------

races_final_df

# COMMAND ----------

write_to_silver(
    input_df = races_final_df,
    target_table = silver_table,
    merge_condition = "t.season = s.season AND t.round = s.round AND t.circuit_id = s.circuit_id",
    columns_to_update = [
        "race_name",
        "date",
        "ingestion_timestamp",
        "source_file",
        "batch_id"
    ]
)

# COMMAND ----------

display(spark.table(silver_table))

# COMMAND ----------

