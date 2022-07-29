# Databricks notebook source
from pyspark.sql import SparkSession

# COMMAND ----------

spark=SparkSession.builder.master("local").appName("Master").getOrCreate()

# COMMAND ----------

df = spark.read.csv('/FileStore/tables/gdp.csv',sep=",",header=True,inferSchema=True)

# COMMAND ----------

df.show(4)

# COMMAND ----------

df.printSchema()

# COMMAND ----------

df.createOrReplaceTempView("GDP")

# COMMAND ----------

spark.sql("select * from GDP").show(5)

# COMMAND ----------

spark.sql("select `Country Code` Code from GDP").show(5)

# COMMAND ----------

spark.sql("select Value Code from GDP").show(5)

# COMMAND ----------

spark.sql("select `Country Code`, length(`Country Code`) from GDP").show(5)

# COMMAND ----------

spark.sql("select * from GDP where Year== 1968 and `Country Name` == 'Arab World'").show(5)

# COMMAND ----------

spark.sql("select * from GDP where Year== 1968  order by Value")

# COMMAND ----------

spark.sql("select mean(Value) from GDP where `Country Code` == 'ARB' ").show()

# COMMAND ----------

spark.sql("select mean(Value) from GDP where `Country Name` in ('Japan', 'North America')").show()

# COMMAND ----------

spark.sql("select Value,`Country Name` from GDP where `Country Name` in ('Japan', 'North America')").show()

# COMMAND ----------

spark.sql("select Value, (Value/10000000) as mul_Value from GDP").show()

# COMMAND ----------

spark.sql("select round(Value,2) from GDP").show()

# COMMAND ----------

