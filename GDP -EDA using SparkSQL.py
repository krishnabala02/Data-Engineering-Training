# Databricks notebook source
from pyspark.sql import SparkSession

# COMMAND ----------

sc=SparkSession.builder.appName('SparkSQL').getOrCreate()

# COMMAND ----------

data=sc.read.csv('/FileStore/tables/gdp.csv',inferSchema=True,header=True)

# COMMAND ----------

data.printSchema()

# COMMAND ----------

data.show(5)

# COMMAND ----------

data.columns

# COMMAND ----------

data.createOrReplaceTempView('GDP')

# COMMAND ----------

sc.sql("select * from GDP").show(5)

# COMMAND ----------

sc.sql("select Value from GDP").show(5)

# COMMAND ----------

sc.sql("select `Country Name` from GDP").show(5)

# COMMAND ----------

sc.sql("select `Country Name` from GDP limit 10").show()

# COMMAND ----------

sc.sql("select `Country Name`, length(`Country Name`) from GDP").show(20)

# COMMAND ----------

sc.sql("select `Country Name`,`Country Code` from GDP  where Year==2000").show()

# COMMAND ----------

sc.sql("select * from GDP  where Year==2000 and `Country Code`=='ECA'").show()

# COMMAND ----------

sc.sql("select * from GDP  where Year==2000 order by Value desc").show()

# COMMAND ----------

sc.sql("select mean(value) from GDP  where `Country Code`=='USA'").show()

# COMMAND ----------

sc.sql("select count(value) from GDP  where `Country Code`=='USA'").show()

# COMMAND ----------

sc.sql("select sum(value) from GDP  where Year==2000 and `Country Name` in ('North America','Japan')").show()

# COMMAND ----------

sc.sql("select mean(value) from GDP  where Year==2000 and `Country Name` in ('North America','Japan')").show()

# COMMAND ----------

sc.sql("select value,`Country Name` from GDP  where Year==2000 and `Country Name` in ('North America','Japan')").show()

# COMMAND ----------

