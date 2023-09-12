# Databricks notebook source
#PYARROW_IGNORE_TIMEZONE=1
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit,col,concat,when,split,length,trim,
import pyspark.sql.functions as ff
import os
from pyspark.sql import window as w
from datetime import datetime
from pyspark.sql.types import StructType, StructField, IntegerType, StringType,TimestampType
import pandas as pd


# COMMAND ----------

spark.conf.set(
    "fs.azure.account.key.a4ladlsgen2.dfs.core.windows.net",
    "FUMJO2tgA5ec3mByXQKx7UqtiSWUfZ6vu6oTyzd+t7ni5HN++agdzq5qblVAkqaVCBplmW7s7oHH+AStG3/Syg==")
df_test=spark.read.csv("abfss://raw@a4ladlsgen2.dfs.core.windows.net/test_dq.csv",header=True)
display(df_test)
jdbcUrl = "jdbc:sqlserver://sqlserver-a4l.database.windows.net:1433;database=Triple-A-DB;user=DEV@sqlserver-a4l;password=P@ssw0rd;encrypt=true;trustServerCertificate=false;hostNameInCertificate=*.database.windows.net;loginTimeout=30;"
df_table = spark.read.format("jdbc") \
    .option("url", jdbcUrl) \
    .option("dbtable", "[dbo].[AAA_orders]") \
    .load()
# df_table=df_table.withColumn("flag",lit(0))
# df_test=df_test.withColumn("flag",lit(0))

# COMMAND ----------

df_test = df_test.withColumn("IsErrorDQ",lit(0))
df_test = df_test.withColumn("DescriptionDQ",lit(' '))

# COMMAND ----------

jdbc_url = "jdbc:sqlserver://sqlserver-a4l.database.windows.net:1433;database=Triple-A-DB"
jdbc_table = "AAA_orders"
jdbc_properties = {
    "user": "DEV",
    "password": "P@ssw0rd",
    "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver"
}

# COMMAND ----------

column_name='menge'

# COMMAND ----------

df_test = df_test.withColumn('menge_clean', split(col(column_name),' ').getItem(0))\
               .withColumn('menge_name', split(col(column_name),' ').getItem(1))\
               .withColumn('menge_clean', ff.regexp_replace(col('menge_clean'), "’", ""))\
               .withColumn('IsErrorDQ', when(col('menge_clean') >= 30000, lit(1)).otherwise(col("IsErrorDQ")))\
               .withColumn("DescriptionDQ",when(col('menge_clean') >= 30000,concat(col("DescriptionDQ"),lit(column_name),lit(' has exceeded the quantity limit, '))).otherwise(col("DescriptionDQ")))\
               .drop('menge')

# COMMAND ----------

display(df_test)

# COMMAND ----------

# Purchase_Order=df_test.select("BestellungNr").distinct().rdd.flatMap(lambda x: x).collect()
# print(Purchase_Order)
# exists = df_table.filter(df_table.BestellungNr==Purchase_Order[0]).count() > 0
# if exists:
#     df_test=df_test.withColumn("concatenated_column",concat(col("Artikel"), col("BestellungNr"),col("Pos")))
#     df_table=df_table.withColumn("concatenated_column",concat(col("Artikel"), col("BestellungNr"),col("Pos")))
#     common_values_df = df_table.join(df_table, on="concatenated_column", how="inner")
#     list_duplicates_id=common_values_df.select("concatenated_column").rdd.flatMap(lambda x: x).collect()
#     if len(list_duplicates_id)>0:
#         df_test = df_test.withColumn("IsErrorDQ", when(col("concatenated_column").isin(list_duplicates_id),1).otherwise(col("IsErrorDQ"))).drop("concatenated_column")
#         df_processed=df_test.filter(df_test.IsErrorDQ==0)
#         df_Error=df_test.filter(df_test.IsErrorDQ==1)
#         display(df_processed)
#         display(df_Error)
#         df_processed.write.jdbc(url=jdbc_url,table=jdbc_table,mode="append",properties=jdbc_properties)
#     else:
#         df_test.write.jdbc(url=jdbc_url,table=jdbc_table,mode="append",properties=jdbc_properties)
         
# else:
#     df_test.write.jdbc(url=jdbc_url,table=jdbc_table,mode="append",properties=jdbc_properties)


# COMMAND ----------

