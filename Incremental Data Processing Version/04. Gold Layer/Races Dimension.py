# Databricks notebook source
dbutils.widgets.text("p_batch_id", "")
v_batch_id = dbutils.widgets.get("p_batch_id")

# COMMAND ----------

# MAGIC %run "../00. Common/GoldHelper"

# COMMAND ----------

# MAGIC %run "../00. Common/Config"

# COMMAND ----------

from pyspark.sql import functions as F

# COMMAND ----------

target_table = f"{catalog_name}.{gold_schema}.dim_races"

# COMMAND ----------


races_df = spark.table(f"{catalog_name}.{silver_schema}.races").filter(F.col("batch_id") == v_batch_id)
circuits_df = spark.table(f"{catalog_name}.{silver_schema}.circuits").filter(F.col("batch_id") == v_batch_id)

# COMMAND ----------

dim_races_df = (
    races_df
        .join(
            circuits_df,
            races_df.circuit_id == circuits_df.circuit_id,
            "inner"
        )
        .select(
            races_df.season,
            races_df.round,
            races_df.race_name,
            races_df.date,
            circuits_df.circuit_name,
            circuits_df.locality,
            circuits_df.country
        )
)

# COMMAND ----------

write_to_gold(
    input_df=dim_races_df,
    target_table=target_table,
    merge_condition="t.season = s.season AND t.round = s.round",
    columns_to_update=[
        "race_name",
        "date",
        "circuit_name",
        "locality",
        "country", 
    ]
)