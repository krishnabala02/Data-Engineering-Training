# Databricks notebook source
from pyspark.sql import SparkSession

# COMMAND ----------

sc=SparkSession.builder.appName("ML").getOrCreate()

# COMMAND ----------

data=sc.read.csv('/FileStore/tables/combined_cycle_power_plant.csv',inferSchema=True,header=True,sep=';')

# COMMAND ----------

data.show(5)

# COMMAND ----------

from pyspark.sql.functions import corr

# COMMAND ----------

data.select(corr('temperature','energy_output')).show()

# COMMAND ----------

data.select(corr('exhaust_vacuum','energy_output')).show()

# COMMAND ----------

data.select(corr('relative_humidity','energy_output',)).show()

# COMMAND ----------

data.select(corr('ambient_pressure','energy_output',)).show()

# COMMAND ----------

from pyspark.ml.feature import VectorAssembler

# COMMAND ----------

data.columns

# COMMAND ----------

ass=VectorAssembler(inputCols=['temperature',
 'exhaust_vacuum',
 'ambient_pressure',
 'relative_humidity'],outputCol='feature')

# COMMAND ----------

finaldata=ass.transform(data)

# COMMAND ----------

finaldata.show(5)

# COMMAND ----------

finaldata=finaldata.select('feature','energy_output')

# COMMAND ----------

finaldata.show(5)

# COMMAND ----------

train_data,test_data=finaldata.randomSplit([0.7,0.3])

# COMMAND ----------

finaldata.count()

# COMMAND ----------

train_data.count()

# COMMAND ----------

test_data.count()

# COMMAND ----------

from pyspark.ml.regression import LinearRegression

# COMMAND ----------

algo=LinearRegression(featuresCol='feature',labelCol='energy_output')

# COMMAND ----------

model=algo.fit(train_data)

# COMMAND ----------

model.coefficients

# COMMAND ----------

model.intercept

# COMMAND ----------

ypred=model.transform(test_data)

# COMMAND ----------

ypred.show(10)

# COMMAND ----------

test_data.show(5)

# COMMAND ----------

test=test_data.select('feature')

# COMMAND ----------

test.show(5)

# COMMAND ----------

model.transform(test).show()

# COMMAND ----------

test2=[[15,78,1585,795]]
test2=sc.createDataFrame(test2,schema=['temperature',
 'exhaust_vacuum',
 'ambient_pressure',
 'relative_humidity'])

# COMMAND ----------

test2=ass.transform(test2)

# COMMAND ----------

model.transform(test2).show()

# COMMAND ----------

