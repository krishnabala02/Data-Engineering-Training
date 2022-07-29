# Databricks notebook source
from pyspark.sql import SparkSession

# COMMAND ----------

spark=SparkSession.builder.master("local").appName("Master").getOrCreate()

# COMMAND ----------

df = spark.read.csv('/FileStore/tables/combined_cycle_power_plant.csv',sep=";",header=True,inferSchema=True)

# COMMAND ----------

df.show()

# COMMAND ----------

from pyspark.sql.functions import corr

# COMMAND ----------

df.select(corr("temperature","energy_output")).show()

# COMMAND ----------

df.select(corr("exhaust_vacuum","energy_output")).show()

# COMMAND ----------

from pyspark.ml.feature import VectorAssembler

# COMMAND ----------

df.columns

# COMMAND ----------

ass=VectorAssembler(inputCols=["temperature","exhaust_vacuum","ambient_pressure","relative_humidity"],outputCol="feature")

# COMMAND ----------

finaldata=ass.transform(df)

# COMMAND ----------

finaldata.show(7)

# COMMAND ----------

finaldata=finaldata.select("feature")

# COMMAND ----------

train_data,test_data=finaldata.randomSplit([0.7,0.3])

# COMMAND ----------

train_data.count()
test_data.count()


# COMMAND ----------

from pyspark.ml.regression import LinearRegression

# COMMAND ----------

algo = LinearRegression(featuresCol="feature",labelCol="energy_output")

# COMMAND ----------

model=algo.fit(train_data)

# COMMAND ----------

model.coefficients

# COMMAND ----------

model.intercept

# COMMAND ----------

