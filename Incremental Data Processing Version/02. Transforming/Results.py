# Databricks notebook source
dbutils.widgets.text("p_batch_id", "")
v_batch_id = dbutils.widgets.get("p_batch_id")

# COMMAND ----------

# MAGIC %run "../00. Common/Config"

# COMMAND ----------

# MAGIC %run "../00. Common/SilverHelper"

# COMMAND ----------

bronze_table = f"{catalog_name}.{bronze_schema}.results"
silver_table = f"{catalog_name}.{silver_schema}.results"


# COMMAND ----------

results_df = spark.table(bronze_table).filter((F.col("batch_id") == v_batch_id))

# COMMAND ----------

display(results_df)

# COMMAND ----------

results_selected_df = results_df.drop("url")

# COMMAND ----------

results_renamed_df = (
    results_selected_df
        .withColumnsRenamed({
            "raceName": "race_name",
            "constructorId": "constructor_id",
            "driverId": "driver_id",
            "date": "race_date",
            "number": "car_number",
            "grid": "grid_position",
            "laps": "completed_laps",
            "position": "final_position",
            "positionText": "final_position_text" 
        })
)

# COMMAND ----------

import pyspark.sql.functions as F
results_valid_df = results_renamed_df.filter(
    F.col("season").isNotNull() &
    F.col("round").isNotNull() &
    F.col("constructor_id").isNotNull() &
    F.col("driver_id").isNotNull()
)

# COMMAND ----------

results_distinct_df = results_valid_df.dropDuplicates(["season", "round", "constructor_id", "driver_id"])

# COMMAND ----------

import pyspark.sql.functions as F
results_final_df = (
    results_distinct_df
        .withColumn('race_name',F.initcap("race_name"))
)

# COMMAND ----------

display(results_final_df)

# COMMAND ----------

write_to_silver(
    input_df = results_final_df,
    target_table = silver_table,
    merge_condition = "t.season = s.season AND t.round = s.round AND t.constructor_id = s.constructor_id AND t.driver_id = s.driver_id",
    columns_to_update = [
        "race_name",
        "race_date",
        "grid_position",
        "completed_laps",
        "car_number",
        "points",
        "final_position",
        "final_position_text",
        "status",
        "ingestion_timestamp",
        "source_file",
        "batch_id"
    ]
)

# COMMAND ----------

display(spark.table(silver_table))