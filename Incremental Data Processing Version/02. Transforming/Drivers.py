# Databricks notebook source
dbutils.widgets.text("p_batch_id", "")
v_batch_id = dbutils.widgets.get("p_batch_id")

# COMMAND ----------

# MAGIC %run "../00. Common/Config"

# COMMAND ----------

# MAGIC %run "../00. Common/SilverHelper"

# COMMAND ----------

bronze_table = f"{catalog_name}.{bronze_schema}.drivers"
silver_table = f"{catalog_name}.{silver_schema}.drivers"


# COMMAND ----------

import pyspark.sql.functions as F

# COMMAND ----------

drivers_df = spark.table(bronze_table).filter((F.col("batch_id") == v_batch_id))

# COMMAND ----------

display(drivers_df)

# COMMAND ----------

drivers_selected_df = drivers_df.drop("url")

# COMMAND ----------

drivers_renamed_df = (
    drivers_selected_df
        .withColumnsRenamed({
            "driverId": "driver_id",
            "dateOfBirth": "date_of_birth"
        })
)

# COMMAND ----------

drivers_concat_df = (
    drivers_renamed_df
        .withColumn("driver_name",
                     F.concat_ws(" ", F.col("name.givenName"), F.col("name.familyName")))
        .drop("name")
)

# COMMAND ----------

display(drivers_concat_df)

# COMMAND ----------

drivers_distinct_df = drivers_concat_df.dropDuplicates(["driver_id"])

# COMMAND ----------

display(drivers_distinct_df)

# COMMAND ----------


drivers_final_df = (
    drivers_distinct_df
        .withColumn('nationality',F.initcap("nationality"))
        .withColumn('driver_name',F.initcap("driver_name"))
)

# COMMAND ----------

write_to_silver(
    input_df = drivers_final_df,
    target_table = silver_table,
    merge_condition = "t.driver_id = s.driver_id",
    columns_to_update = [
        "date_of_birth",
        "nationality",
        "driver_name",
        "ingestion_timestamp",
        "source_file",
        "batch_id"
    ]
)

# COMMAND ----------

display(spark.table(silver_table))

# COMMAND ----------



# COMMAND ----------

