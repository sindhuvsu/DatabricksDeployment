# Databricks notebook source
print('test')

# COMMAND ----------

spark.conf.set("spark.databricks.delta.enabled", "false")


# COMMAND ----------

!pip install azure-identity

# COMMAND ----------

!pip install azure-storage-blob


# COMMAND ----------

def concat(start,end,df,column_name):
    text='' 
    while start<end:
        # print(df[column_name][start])
        text = text+' '+ df[column_name][start]
        start+= 1
        text=text.strip()
    return text

# COMMAND ----------

# Tenant ID for your Azure Subscription
TENANT_ID = "865cc515-a530-4538-8ef8-072b7b2be759"
# Your Service Principal App ID (Client ID)
CLIENT_ID = "2c06cbbb-3d10-4005-8691-715b198c28aa"
# Your Service Principal Password (Client Secret)
CLIENT_SECRET = "rll8Q~wiL1NH7EtZnjQa_ssHrU4lWg4grf4zDct~"
ACCOUNT_NAME = "a4ladlsgen2"
CONTAINER_NAME = "raw"


# COMMAND ----------

spark.conf.set(
    "fs.azure.account.key.a4ladlsgen2.dfs.core.windows.net",
    "FUMJO2tgA5ec3mByXQKx7UqtiSWUfZ6vu6oTyzd+t7ni5HN++agdzq5qblVAkqaVCBplmW7s7oHH+AStG3/Syg==")


# COMMAND ----------

dbutils.fs.ls("abfss://raw@a4ladlsgen2.dfs.core.windows.net/")

# COMMAND ----------

#Read the File as a dataframe
df_test=spark.read.csv("abfss://raw@a4ladlsgen2.dfs.core.windows.net/test_dq.csv",header=True)
display(df_test)

# COMMAND ----------

import json
import pandas as pd
from azure.identity import ClientSecretCredential
from azure.storage.blob import BlobServiceClient

# COMMAND ----------

table_name="customBestellung_924874"

# COMMAND ----------

Metadata = {
  "FMC_table" : {
    "columns_dict" : {'Delivery_Date.content': 'Delivery Date', 
              'Item.content': 'Item',
              'Material_Number/Description.content': 'Material_Number/Description', 
              'Net_Price.content': 'Net Price',
              'Order_Unit.content': 'Order Unit',
              'Order_Quantity.content': 'Order Quantity',  
              'Purchasing_Document.content': 'Purchasing Document',
              'Unit_Price/Currency.content': 'Unit_Price/Currency',
              'Vendor_Name.content': 'Vendor_Name',
              'Vendor_Number.content': 'Vendor_Number'},
    "transform_columns" : ["Purchasing Document","Vendor_Name","Vendor_Number"],
    "key_column":"Item"
  },
  "customBestellung_924874":
  {
    "columns_dict" : {
                "Belegdatum.content": "Belegdatum",
                "Ansprechpartner.content": "Ansprechpartner",
                "Telefonnr.direkt.content":"Telefonnr.direkt",
                "E-Mail.content": "E-Mail",
                "UnsereKunden-Nr.content": "UnsereKunden-Nr",
                "IhreKontaktperson.content": "IhreKontaktperson",
                "Bestelldaten.content": "Bestelldaten",
                "Liefertermin.content": "Liefertermin",
                "BestellungNr.content": "Bestellung Nr",
                "Pos.content":"Pos",
                "Artikel.content":"Artikel",
                "Menge.content": "Menge",
                "Preis/ME.content": "Preis/ME",
                "ArticleDescription.content":"ArticleDescription",
                "Gesamtpreis.content":"Gesamtpreis",
                "Format.content":"Format",
                "Form.content":"Form",
                "Trägerbandbreite.content":"Trägerbandbreite",
                "Druck.content":"Druck",
                "FarbenVS.content":"FarbenVS",
                "FarbenRS.content":"FarbenRS",
                "Material1.content":"Material1",
                "Material2.content":"Material2",
                "Trägermaterial.content":"Trägermaterial",
                "Aufmachung.content":"Aufmachung",
                "Rollendurchmesser.content":"Rollendurchmesser",
                "Rollenà.content":"Rollenà",
                "Kerndurchmesser.content":"Kerndurchmesser",
                "Wickelrichtung.content":"Wickelrichtung",
                "Verpackung.content":"Verpackung",
                "Variantarticlenumber.content":"Variantarticlenumber",
                "Variantdescription.content":"Variantdescription",
                "SummeEUR.content":"SummeEUR",
                "NettowertEUR.content":"NettowertEUR",
                "MwSt.content":"MwSt",
                "VariantArticleDescription.content":"VariantArticleDescription",
                "GesamtbetragEUR.content":"Gesamtbetrag(EUR)"

    },
    "transform_columns" : ["Belegdatum","Bestellung Nr","Ansprechpartner","Telefonnr.direkt","E-Mail","UnsereKunden-Nr","IhreKontaktperson","Bestelldaten","Liefertermin","SummeEUR","NettowertEUR","MwSt","Gesamtbetrag(EUR)"],
    "key_column":"Artikel"
  }
}
column_names=Metadata[table_name]['columns_dict']
transform_columns=Metadata[table_name]["transform_columns"]
primary_key=Metadata[table_name]["key_column"]
print("")
# print(transform_columns)

# COMMAND ----------

credentials = ClientSecretCredential(TENANT_ID, CLIENT_ID, CLIENT_SECRET)
blobService = BlobServiceClient(
   "https://{}.blob.core.windows.net".format(ACCOUNT_NAME),
   credential=credentials
)
print("\n==============LIST OF ALL BLOBS=================")
# Path in the container. If you want to list everything in the root path, keep it empty
prefix = ""
container_client = blobService.get_container_client(CONTAINER_NAME)
directory_path = "all4l-AAA/"
blobs = container_client.list_blobs(name_starts_with=directory_path)

# COMMAND ----------

# Loop through each blob in the directory
for blob in blobs:
    # Get the blob name
    blob_name = blob.name    
    # Read the contents of the JSON file
    blob_client = container_client.get_blob_client(blob_name)
    file_contents = blob_client.download_blob().readall()
    # type(file_contents)
    # Decode the contents as a JSON object
    json_data = file_contents.decode("utf-8")
    data_dict = json.loads(file_contents)
    print(data_dict)
    # data = data_dict['body']['analyzeResult']['documents'][0]['fields'][table_name]['   ']
    data = data_dict['analyzeResult']['documents'][0]['fields'][table_name]['valueArray']
    count =len(pd.json_normalize(data).index)
    df=pd.DataFrame()
    for i in range(count):
        d1 = data[i]['valueObject']
        df1 = pd.json_normalize(d1)
        df = pd.concat([df, df1], sort=False,ignore_index=False)
    df=df.filter(regex='content')
    df = df.reset_index(drop = True)
    df=df.rename(columns=column_names)
    display(df)
    df=df.fillna('');
    column_headers = list(df.columns.values)
    # print(column_headers)
    column_headers.remove(primary_key)
    # column_headers = [x for x in column_headers if x not in transform_columns]
    # print(column_headers)
    p_list = df[primary_key].tolist()
    # # print(p_list)
    res = [i for i, val in enumerate(p_list) if val != '']
    # print(res)
    res.append(len(df))
    for i in range(len(res)-1):
                    # print(ind)
        for column_name in column_headers:
            df[column_name].fillna('', inplace=True)
            # df[column_name][res[i]+1]!=''
            result=''
                        # print(res[i])
            result=concat(res[i],res[i+1],df,column_name)
    #                     print(result)
            df[column_name][res[i]]=result
            # display(df)   
    # print(primary_key)
    df2 = df[df[primary_key]!='']
    # display(df2)
df2=df2.replace('',None)
display(df2)
for i in transform_columns:
    # print(i)
    non_null_value= df2[i].dropna().iloc[0]
    # print( non_null_value)
    df2[i]=non_null_value
display(df2)
# csv_data = df.to_csv(index=False)
df_csv=spark.createDataFrame(df2)
# display(df_csv)
df_csv = df_csv.coalesce(1)
df_csv.write.format("csv").mode("overwrite").option("header", "true").save("abfss://raw@a4ladlsgen2.dfs.core.windows.net/target")
# df_csv.write.mode('overwrite').option("header",True).csv("abfss://raw@a4ladlsgen2.dfs.core.windows.net/target")
# df2.write.format("csv").mode('overwrite').save("/tmp/spark_output/zipcodes")
# df2.write.mode("overwrite").format("com.databricks.spark.csv").option("header", "true").csv("adl://<your-

# COMMAND ----------

#PYARROW_IGNORE_TIMEZONE=1
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit,col,concat,when,split,length,trim
import pyspark.sql.functions as ff
import os
from pyspark.sql import window as w
from datetime import datetime
from pyspark.sql.types import StructType, StructField, IntegerType, StringType,TimestampType
import pandas as pd


# COMMAND ----------

dbutils.secrets.help()

# COMMAND ----------

dbutils.widgets.text("Reporting_Year","")
dbutils.widgets.text("Reporting_Month","")
dbutils.widgets.text("Customer_Name","")
dbutils.widgets.text("File_Format","")

# COMMAND ----------

# Accessing the parameter value
Reporting_Year = dbutils.widgets.get("Reporting_Year")
Reporting_Month = dbutils.widgets.get("Reporting_Month")
Customer_Name = dbutils.widgets.get("Customer_Name")
File_Format= dbutils.widgets.get("File_Format")
# File_Name= dbutils.widgets.get("File_Name")
# PipelineName= dbutils.widgets.get("PipelineName")
# PipelineRunId= dbutils.widgets.get("PipelineRunId")

# COMMAND ----------

jdbcUrl = "jdbc:sqlserver://sqlserver-a4l.database.windows.net:1433;database=Triple-A-DB;user=DEV@sqlserver-a4l;password=P@ssw0rd;encrypt=true;trustServerCertificate=false;hostNameInCertificate=*.database.windows.net;loginTimeout=30;"
# Read data from Azure SQL Database
df_rules = spark.read.format("jdbc") \
    .option("url", jdbcUrl) \
    .option("dbtable", "[dbo].[DQ_Control_Table]") \
    .load()

# COMMAND ----------

def NULL_CHECK(df,column_name):
    df = df.withColumn("IsErrorDQ",when((col(column_name).isNull()==1) | (ff.col("IsErrorDQ")==1),1).otherwise(0))
    df = df.withColumn("DescriptionDQ",when(col(column_name).isNull()==1,concat(col("DescriptionDQ"),lit(column_name),lit(' Contains NULL, '))).otherwise(col("DescriptionDQ")))

    return df

def UNIQUENESS(df,column_name):
    col_names = [i for i in column_name.split(',')]
    df = df.withColumn('concatenated_cols',concat(*col_names))
    windowSpec = w.Window.partitionBy("concatenated_cols")
    df = df.withColumn("IsErrorDQ", ff.when((ff.count('*').over(windowSpec) > 1) | (ff.col("IsErrorDQ")==1), 1).otherwise(0))
    df = df.withColumn("DescriptionDQ", ff.when(ff.count('*').over(windowSpec) > 1, concat(col("DescriptionDQ"),lit(column_name),lit(' has duplicate values, '))).otherwise(col("DescriptionDQ"))).drop("concatenated_cols")
    #df.show()
    return df

def EXISTENCE(df,column_name,df_reference):
    result=df.join(df_reference, column_name, 'semi') \
    .distinct() \
    .select(column_name,lit('').alias('DescriptionDQ_reference'))\
    .join(df,column_name, 'right') \
    .fillna("Article_reference doesn't exist in the erp table,", 'DescriptionDQ_reference')\
    .distinct()
    result=result.withColumn('DescriptionDQ',concat(col('DescriptionDQ'),lit(""),col('DescriptionDQ_reference'))).drop(result.DescriptionDQ_reference)
    result = result.withColumn("IsErrorDQ",when((col("DescriptionDQ").contains(" Article_reference doesn't exist in the erp table, ")) | (col("IsErrorDQ")==1),1).otherwise(0))
    return result

def LENGTH_CHECK(df,column_name,column_length):
    df = df.withColumn("IsErrorDQ",when((length(trim(col(column_name)))!=column_length) | (ff.col("IsErrorDQ")==1),1).otherwise(0))
    df = df.withColumn("DescriptionDQ",when(length(trim(col(column_name)))!=column_length,concat(col("DescriptionDQ"),lit(column_name),lit(' has incorrect character length, '))).otherwise(col("DescriptionDQ")))
    return df
def Quantity_Limit(df,Limit_Value):
    df = df.withColumn('menge_clean', split(col('menge'),' ').getItem(0))\
                                .withColumn('menge_name', split(col('menge'),' ').getItem(1))\
                                .withColumn('menge_clean', regexp_replace(col('menge_clean'), "’", ""))\
                                .withColumn('isActive', when(col('menge_clean') >= 30000, lit(1)).otherwise(''))\
                               .drop('menge')
    

# COMMAND ----------

df_csv = df_csv.withColumn("IsErrorDQ",lit(0))
df_csv = df_csv.withColumn("DescriptionDQ",lit(' '))

# COMMAND ----------

display(df_rules)
rules = df_rules.withColumn("CheckType",ff.explode(ff.split('CheckType',';')))
for rule in rules.collect():
    if rule["CheckType"] == 'NULL_CHECK':
        df_csv = NULL_CHECK(df_csv,rule["Column_Name"])
        print("yes")
    elif rule["CheckType"] == 'UNIQUENESS':
        df_csv = UNIQUENESS(df_csv,rule["Column_Name"])
    elif rule["CheckType"] == 'LENGTH_CHECK':
        df_csv = LENGTH_CHECK(df_csv,rule["Column_Name"],rule["Rule_Support_Value"])
display(df_csv)

# COMMAND ----------

df_test = df_test.withColumn("IsErrorDQ",lit(0))
df_test = df_test.withColumn("DescriptionDQ",lit(' '))
display(df_rules)
rules = df_rules.withColumn("CheckType",ff.explode(ff.split('CheckType',';')))
for rule in rules.collect():
    if rule["CheckType"] == 'NULL_CHECK':
        df_test = NULL_CHECK(df_test,rule["Column_Name"])
        print("yes")
    elif rule["CheckType"] == 'UNIQUENESS':
        df_test = UNIQUENESS(df_test,rule["Column_Name"])
    elif rule["CheckType"] == 'LENGTH_CHECK':
        df_test = LENGTH_CHECK(df_test,rule["Column_Name"],rule["Rule_Support_Value"])
display(df_test)


# COMMAND ----------

df_table = spark.read.format("jdbc") \
    .option("url", jdbcUrl) \
    .option("dbtable", "[dbo].[AAA_orders]") \
    .load()
df_table=df_table.withColumn("flag",lit(0))
df_csv=df_csv.withColumn("flag",lit(0))

# COMMAND ----------

display(df_csv)
display(df_table)



# COMMAND ----------

# MAGIC %md
# MAGIC common_values_df = df_csv.join(df_table, on="Bestellung Nr", how="inner")
# MAGIC display(common_values_df)
# MAGIC list_duplicates_id=common_values_df.select("Bestellung Nr").rdd.flatMap(lambda x: x).collect()
# MAGIC print(list_duplicates_id

# COMMAND ----------

df_csv=df_csv.withColumn("concatenated_column",concat(col("Artikel"), col("Bestellung Nr")))
df_table=df_table.withColumn("concatenated_column",concat(col("Artikel"), col("Bestellung Nr")))
display(df_table)
# Create a new DataFrame with the common values in the specific column
common_values_df = df_csv.join(df_table, on="concatenated_column", how="inner")
display(common_values_df)
list_duplicates_id=common_values_df.select("concatenated_column").rdd.flatMap(lambda x: x).collect()
print(list_duplicates_id)
type(list_duplicates_id)
# print(len(list_duplicates_id))
if len(list_duplicates_id) != 0:
    df_table = df_table.withColumn("flag", when(col("concatenated_column").isin(list_duplicates_id),1).otherwise(0)).drop("concatenated_column")
    df_csv = df_csv.withColumn("flag", when(col("concatenated_column").isin(list_duplicates_id),1).otherwise(0)).drop("concatenated_column")
else:
    print("There are no dupicates in the table")
display(df_table)
df_table=df_table.filter(df_table.flag==1)
display(df_table)
jdbc_url = "jdbc:sqlserver://sqlserver-a4l.database.windows.net:1433;database=Triple-A-DB"
jdbc_table = "AAA_orders"
jdbc_properties = {
    "user": "DEV",
    "password": "P@ssw0rd",
    "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver"
}
# Write the DataFrame to the database table
df_csv.write.jdbc(url=jdbc_url,
              table=jdbc_table,
              mode="append",  # Use "overwrite" to replace the existing table, or "append" to append to it
              properties=jdbc_properties)
# df_table.write.jdbc(url=jdbc_url,
#               table=jdbc_table,
#               mode="append",  # Use "overwrite" to replace the existing table, or "append" to append to it
#               properties=jdbc_properties)



# COMMAND ----------

display(df_csv)

# COMMAND ----------

# MAGIC %md
# MAGIC df_csv.join(concatenated_df, df_csv.select(concat(col("Artikel"), col("Bestellung Nr")))
# MAGIC  == concatenated_df['concatenated_column'], 'left')

# COMMAND ----------

display(df_table)

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------

