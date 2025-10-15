# Databricks notebook source
# MAGIC %md
# MAGIC ###SILVER _LAYER SCRIPT_

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data Access Using App

# COMMAND ----------

spark.conf.set("fs.azure.account.auth.type.staticdatalake.dfs.core.windows.net", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type.staticdatalake.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id.staticdatalake.dfs.core.windows.net", "b4002138-c46e-4672-a31f-b4d3cc20ccee")
spark.conf.set("fs.azure.account.oauth2.client.secret.staticdatalake.dfs.core.windows.net", "Sql8Q~Ax69Mkrb5jxzYl.wDztEojNLciU9kg2dnz")
spark.conf.set("fs.azure.account.oauth2.client.endpoint.staticdatalake.dfs.core.windows.net", "https://login.microsoftonline.com/0b4490f1-62f2-4a80-bf63-3740c9fe2ee2/oauth2/token")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Data Loading

# COMMAND ----------

df_cal=spark.read.format('csv').option("header",True).option("inferSchema",True).load('abfss://bronze@staticdatalake.dfs.core.windows.net/AdventureWorks_Calendar')

# COMMAND ----------

df_cus=spark.read.format('csv')\
    .option("header",True)\
    .option("inferSchema",True).\
    load('abfss://bronze@staticdatalake.dfs.core.windows.net/AdventureWorks_Customers')

# COMMAND ----------

df_procat=spark.read.format('csv')\
    .option("header",True)\
    .option("inferSchema",True).\
    load('abfss://bronze@staticdatalake.dfs.core.windows.net/AdventureWorks_Product_Categories')

# COMMAND ----------

df_prosubcat=spark.read.format('csv')\
    .option("header",True)\
    .option("inferSchema",True).\
    load('abfss://bronze@staticdatalake.dfs.core.windows.net/Product_Subcategories')

# COMMAND ----------

df_pro=spark.read.format('csv')\
    .option("header",True)\
    .option("inferSchema",True).\
    load('abfss://bronze@staticdatalake.dfs.core.windows.net/AdventureWorks_Products')

# COMMAND ----------

df_returns=spark.read.format('csv')\
    .option("header",True)\
    .option("inferSchema",True).\
    load('abfss://bronze@staticdatalake.dfs.core.windows.net/AdventureWorks_Returns')

# COMMAND ----------

df_sales=spark.read.format('csv')\
    .option("header",True)\
    .option("inferSchema",True).\
    load('abfss://bronze@staticdatalake.dfs.core.windows.net/AdventureWorks_Sales*')

# COMMAND ----------

df_terr=spark.read.format('csv')\
    .option("header",True)\
    .option("inferSchema",True).\
    load('abfss://bronze@staticdatalake.dfs.core.windows.net/AdventureWorks_Territories')

# COMMAND ----------

# MAGIC %md
# MAGIC ##Transformation

# COMMAND ----------

df_cal.display()

# COMMAND ----------

df_cal = df.withColumn("Month",month(col('Date')))\
           .withColumn('Year',year(col('Date')))\
           .withColumn('Day',dayofmonth(col('Date')))
df_cal.display()

# COMMAND ----------

df_cal.write.format('parquet')\
    .mode('append')\
    .option('path',"abfss://silver@staticdatalake.dfs.core.windows.net/AdventureWorks_Calendar")\
    .save()

# COMMAND ----------

df_cus.display()

# COMMAND ----------

df_cus.withColumn("FullName", concat(col('Prefix'),lit(' '), col('FirstName'),lit(' '), col('FirstName'))).display()


# COMMAND ----------

df_cus=df_cus.withColumn('FullName',concat_ws(' ',col('Prefix'),col('FirstName'),col('LastName')))

# COMMAND ----------

df_cus.display()

# COMMAND ----------

df_cus.write.format('parquet')\
    .mode('append')\
    .option('path',"abfss://silver@staticdatalake.dfs.core.windows.net/AdventureWorks_Customers")\
    .save()

# COMMAND ----------

df_procat.display()

# COMMAND ----------

df_procat.write.format('parquet')\
    .mode('append')\
    .option('path',"abfss://silver@staticdatalake.dfs.core.windows.net/AdventureWorks_Product_Categories")\
    .save()

# COMMAND ----------

df_prosubcat.display()

# COMMAND ----------

df_prosubcat.write.format('parquet')\
    .mode('append')\
    .option('path',"abfss://silver@staticdatalake.dfs.core.windows.net/Product_Subcategories")\
    .save()

# COMMAND ----------

df_pro.display()

# COMMAND ----------

df_pro= df_pro.withColumn('ProductSKU',split(col('ProductSKU'),'-')[0])\
              .withColumn('ProductName',split(col('ProductName'),' ')[0])

# COMMAND ----------

df_pro.display()

# COMMAND ----------

df_pro.write.format('parquet')\
    .mode('append')\
    .option('path',"abfss://silver@staticdatalake.dfs.core.windows.net/Products")\
    .save()

# COMMAND ----------

df_returns.display()

# COMMAND ----------

df_returns.write.format('parquet')\
    .mode('append')\
    .option('path',"abfss://silver@staticdatalake.dfs.core.windows.net/AdventureWorks_Returns")\
    .save()

# COMMAND ----------

df_terr.display()

# COMMAND ----------

df_terr.write.format('parquet')\
    .mode('append')\
    .option('path',"abfss://silver@staticdatalake.dfs.core.windows.net/AdventureWorks_Territories")\
    .save()

# COMMAND ----------

df_sales.display()

# COMMAND ----------

df_sales=df_sales.withColumn('StockDate',to_timestamp(col('StockDate')))

# COMMAND ----------

df_sales=df_sales.withColumn('OrderNumber',regexp_replace(col('OrderNumber'),'S','T'))

# COMMAND ----------

df_sales=df_sales.withColumn('Multiply',col('OrderLineItem')*col('OrderQuantity'))

# COMMAND ----------

df_sales.display()

# COMMAND ----------

df_sales.write.format('parquet')\
    .mode('append')\
    .option('path',"abfss://silver@staticdatalake.dfs.core.windows.net/AdventureWorks_Sales")\
    .save()

# COMMAND ----------

# MAGIC %md SALES ANALYSIS
# MAGIC

# COMMAND ----------

df_sales.groupby('OrderDate').agg(count('OrderNumber').alias('Total_Orders')).display()

# COMMAND ----------

df_procat.display()

# COMMAND ----------

df_terr.display()