# Databricks notebook source
dbutils.widgets.text("p_batch_id", "")
v_batch_id = dbutils.widgets.get("p_batch_id")

# COMMAND ----------

# MAGIC %run "../00. Common/Config"

# COMMAND ----------

# MAGIC %run "../00. Common/BronzeHelpers"

# COMMAND ----------

source_file = f"{landing_folder_path}/{v_batch_id}/constructors.json"
table_name = f"{catalog_name}.{bronze_schema}.constructors"

# COMMAND ----------

constructors_schema = "constructorId STRING, name STRING, nationality STRING, url STRING"

# COMMAND ----------

constructors_df = (
    spark.read
        .format('json')
        .schema(constructors_schema)
        .option('mode', "FAILFAST")
        .load(source_file)
)


# COMMAND ----------

constructors_f_df = add_ingestion_metadata(constructors_df)

# COMMAND ----------

display(constructors_f_df)

# COMMAND ----------

write_to_bronze(
    input_df = constructors_f_df,
    table_name = table_name,
    batch_id = v_batch_id
)

# COMMAND ----------

display(spark.table(table_name))