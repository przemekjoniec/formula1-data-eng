# Databricks notebook source
dbutils.widgets.text("p_batch_id", "")
v_batch_id = dbutils.widgets.get("p_batch_id")

# COMMAND ----------

# MAGIC %run "../00. Common/Config"

# COMMAND ----------

# MAGIC %run "../00. Common/SilverHelper"

# COMMAND ----------

bronze_table = f"{catalog_name}.{bronze_schema}.constructors"
silver_table = f"{catalog_name}.{silver_schema}.constructors"


# COMMAND ----------

constructors_df = spark.table(bronze_table).filter((F.col("batch_id") == v_batch_id))

# COMMAND ----------

constructors_selected_df = constructors_df.drop("url")

# COMMAND ----------

constructors_renamed_df = (
    constructors_selected_df
        .withColumnsRenamed({
            "constructorId": "constructor_id",
        })
)

# COMMAND ----------

constructors_distinct_df = constructors_renamed_df.dropDuplicates(["constructor_id"])

# COMMAND ----------

display(constructors_distinct_df)

# COMMAND ----------

import pyspark.sql.functions as F
constructors_final_df = (
    constructors_distinct_df
        .withColumn('nationality',F.initcap("nationality"))
)

# COMMAND ----------

display(constructors_final_df)

# COMMAND ----------

write_to_silver(
    input_df = constructors_final_df,
    target_table = silver_table,
    merge_condition = "t.constructor_id = s.constructor_id",
    columns_to_update = [
        "name",
        "nationality",
        "ingestion_timestamp",
        "source_file",
        "batch_id",
    ]
)

# COMMAND ----------

display(spark.table(silver_table))