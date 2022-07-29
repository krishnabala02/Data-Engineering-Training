# Databricks notebook source
from pyspark import SparkConf,SparkContext 

# COMMAND ----------

conf =SparkConf().setAppName("RDD")

# COMMAND ----------

sc=SparkContext(conf=conf)

# COMMAND ----------

data=["spark is a solution for bigdata","effregtrgtrhr"]

# COMMAND ----------

rdd=sc.parallelize(data)

# COMMAND ----------

rdd

# COMMAND ----------

rdd.collect()

# COMMAND ----------

def upper(s):
    return s.upper()

# COMMAND ----------

strrdd = rdd.map(upper)

# COMMAND ----------

strrdd.collect()

# COMMAND ----------

data1=[6,9,0,7,54,99,2,34]
rdd=sc.parallelize(data1)

# COMMAND ----------

rdd2=rdd.filter(lambda x:x%2==0)

# COMMAND ----------

rdd2.collect()

# COMMAND ----------

 