# Databricks notebook source
from pyspark.sql import SparkSession


# COMMAND ----------

spark=SparkSession.builder.master("local").appName("Master").getOrCreate()

# COMMAND ----------

data=[("spark",45786),("scala",78899),("python",8888),("sql",8999)]

# COMMAND ----------

df=spark.createDataFrame(data)

# COMMAND ----------

df.show()

# COMMAND ----------

type(df)

# COMMAND ----------

df = spark.read.csv('/FileStore/tables/insurance.csv',sep=",",header=True,inferSchema=True)

# COMMAND ----------

df.show()

# COMMAND ----------

df.head(5)

# COMMAND ----------

df.describe().show()

# COMMAND ----------

df.dtypes

# COMMAND ----------

df.printSchema()

# COMMAND ----------

df.distinct().count()

# COMMAND ----------

df1=spark.createDataFrame(df.head(5))

# COMMAND ----------

df1.show()

# COMMAND ----------

df1.count()

# COMMAND ----------

df.describe("age").show()

# COMMAND ----------

df.select("age","smoker","charges").show(5)

# COMMAND ----------

df.select("age").count()

# COMMAND ----------

df.select("age").distinct().count()

# COMMAND ----------

df.select("age").where(df.select("smoker") == "yes")

# COMMAND ----------

df.show()

# COMMAND ----------

df.select("age","smoker").filter(df.smoker == "yes").show()

# COMMAND ----------

df.filter((df.age == 30) & (df.smoker <> "yes")) 

# COMMAND ----------

df.filter(df.age == 30).show()

# COMMAND ----------

df.filter((df.age == 30) & (df.smoker == "yes")).show()

# COMMAND ----------

df.age

# COMMAND ----------

df.filter((df.age == 30) & (df.smoker == "yes")).show()

# COMMAND ----------

lis=[0,3,1]
df.filter(df.children.isin(lis)).show()

# COMMAND ----------

df.filter(df.sex.startswith("f")).show()

# COMMAND ----------

df.orderBy("age").show()

# COMMAND ----------

df.groupBy("smoker").count().show()

# COMMAND ----------

df.select("region").groupBy("smoker").show()

# COMMAND ----------

df.groupBy("smoker").mean("age","bmi","charges").show()

# COMMAND ----------

df.groupBy("children").max("charges").show()

# COMMAND ----------

df.select(df.age).show()

# COMMAND ----------

df.filter((df["age"] == 30) & (df["smoker"] == "yes")).show()

# COMMAND ----------

df.where((df.age>30) & (df.charges >60000).show()

# COMMAND ----------

df.select("age").na

# COMMAND ----------

df.where(df["age"].isNULL()).count()

# COMMAND ----------

df.dropna(subset=["age"])

# COMMAND ----------

df2 = spark.read.csv('/FileStore/tables/insurance_miss.csv',sep=",",header=True,inferSchema=True)

# COMMAND ----------

df2.show()

# COMMAND ----------

a= df2.columns
for i in a:
    print(str(i),df2.where(df2[i].isNull()).count())

# COMMAND ----------

df3 = df2.fillna(0,subset=["charges"])

# COMMAND ----------

df4 = df2.dropna(subset=["bmi"]).count()

# COMMAND ----------

print(df4)

# COMMAND ----------

print(df2.count())

# COMMAND ----------

df5 = df2.fillna(1,subset=["children"])

# COMMAND ----------

df.groupBy("smoker","sex").pivot("region").avg("charges").show()

# COMMAND ----------

df=df.withColumnRenamed("age", "Age")

# COMMAND ----------

df.describe().show()

# COMMAND ----------

from pyspark.sql.functions import corr

# COMMAND ----------

df.show()

# COMMAND ----------

old=df.columns
new=['Age', 'Sex', 'Bmi', 'Children', 'Smoker', 'Region', 'Charges']
for a,b in zip(old,new):
  df=df.withColumnRenamed(a,b)
df.show(5)

# COMMAND ----------

data = df3.withColumn("newcolumn",df3["age"]+df3["bmi"])
data.show()

# COMMAND ----------

df.show()