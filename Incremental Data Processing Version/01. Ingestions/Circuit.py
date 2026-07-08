# Databricks notebook source
dbutils.widgets.text("p_batch_id", "")
v_batch_id = dbutils.widgets.get("p_batch_id")

# COMMAND ----------

# MAGIC %run "../00. Common/Config"

# COMMAND ----------

# MAGIC %run "../00. Common/BronzeHelpers"

# COMMAND ----------

source_file = f"{landing_folder_path}/{v_batch_id}/circuits.csv"
table_name = f"{catalog_name}.{bronze_schema}.circuits"

# COMMAND ----------

table_name

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, DoubleType

circuits_schema = StructType([
    StructField('circuitId', StringType()),
    StructField('url', StringType()),
    StructField('circuitName', StringType()),
    StructField('lat', DoubleType()),
    StructField('long', DoubleType()),
    StructField('locality', StringType()),
    StructField('country', StringType())
])


# COMMAND ----------

circuits_df = (
    spark.read
    .format('csv')
    .option('header','true')
   # .option('inferSchema','true')
    .option('mode','PERMISSIVE')
    .schema(circuits_schema)
    .load(source_file)
)


# COMMAND ----------

circuits_df.show()

# COMMAND ----------

circuits_f_df = add_ingestion_metadata(circuits_df)

# COMMAND ----------

display(circuits_f_df)

# COMMAND ----------

table_name

# COMMAND ----------

#circuits_f_df = circuits_f_df.withColumn("batch_id", F.lit(v_batch_id))

#(
#    circuits_f_df
#    .write
#    .format('delta')
#    .mode('overwrite')
#    .partitionBy('batch_id')
#    .option('replaceWhere', f"batch_id = '{v_batch_id}'")
#    .saveAsTable(table_name)
#)

write_to_bronze(
    input_df = circuits_f_df,
    table_name = table_name,
    batch_id = v_batch_id
)

# COMMAND ----------

display(spark.table(table_name))