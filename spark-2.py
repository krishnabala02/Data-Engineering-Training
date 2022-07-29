# Databricks notebook source
from pyspark.sql import SparkSession

# COMMAND ----------

spark=SparkSession.builder.master("local").appName("Master").getOrCreate()

# COMMAND ----------

df = spark.read.csv('/FileStore/tables/Bank-1.csv',sep=",",header=True,inferSchema=True)

# COMMAND ----------

df.show()

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

df.describe("Balance").show()

# COMMAND ----------

df.select("CreditScore","Geography","CustomerId").show(5)

# COMMAND ----------

df.select("CustomerId").where(df["CreditScore"] > 460).show()

# COMMAND ----------

df.select("Geography","Balance","CustomerId").filter(df.IsActiveMember == 1).show()

# COMMAND ----------

df.filter((df.CreditScore > 600) & (df.IsActiveMember == 1)).show()

# COMMAND ----------

df.groupBy("Geography").count().show()

# COMMAND ----------

df.groupBy("Geography").mean("Balance","EstimatedSalary").show()

# COMMAND ----------

a= df.columns
for i in a:
    print(str(i),df.where(df[i].isNull()).count())

# COMMAND ----------

df1= spark.read.csv('/FileStore/tables/Bank.csv',sep=",",header=True,inferSchema=True)

# COMMAND ----------

old=df1.columns[0:3]
print(old)
new=['customerId', 'surname', 'creditScore']
for a,b in zip(old,new):
    df1=df1.withColumnRenamed(a,b)
df1.show(5)

# COMMAND ----------

data = df1.withColumn("percentsaving",df1["Balance"]/df1["EstimatedSalary"] * 100)
data.show()


# COMMAND ----------

df.groupBy("Geography").pivot("Gender").max("creditScore").show()

# COMMAND ----------

 df.groupBy("Geography").pivot("Exited").count().show()



# COMMAND ----------

df.show()

# COMMAND ----------

/FileStore/tables/sales_data.csv
/FileStore/tables/zomato.csv
/FileStore/tables/CountryCode.csv
/FileStore/tables/appl_stock.csv