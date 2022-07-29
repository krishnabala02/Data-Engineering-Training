# Databricks notebook source
from pyspark.sql import SparkSession

# COMMAND ----------

spark=SparkSession.builder.master("local").appName("spark_df").getOrCreate()

# COMMAND ----------

df = spark.read.csv('/FileStore/tables/Bank-1.csv',sep=",",header=True,inferSchema=True)

# COMMAND ----------

df.show(4)

# COMMAND ----------

df.columns

# COMMAND ----------

df.count()

# COMMAND ----------

df.dtypes

# COMMAND ----------

df.printSchema()

# COMMAND ----------

df.select("CustomerId").show(5)

# COMMAND ----------

df.filter(df.Age>50).where(df.Exited == 0).show(6)

# COMMAND ----------

df.filter(df.Age>50).where(df.Exited == 0).count()

# COMMAND ----------

simpleData = [("James","Sales","NY",90000,34,10000), 
    ("Michael","Sales","NY",86000,56,20000), 
    ("Robert","Sales","CA",81000,30,23000), 
    ("Maria","Finance","CA",90000,24,23000) 
  ]


# COMMAND ----------

simpleData2 = [("James","Sales","NY",90000,34,10000), 
    ("Maria","Finance","CA",90000,24,23000), 
    ("Jen","Finance","NY",79000,53,15000), 
    ("Jeff","Marketing","CA",80000,25,18000), 
    ("Kumar","Marketing","NY",91000,50,21000) 
  ]

# COMMAND ----------

#sh=df.schema
new_df=spark.createDataFrame(df.head(5))
rdd=spark.sparkContext.parallelize(new_df)

# COMMAND ----------

#new_df=spark.createDataFrame(df,schema=sh)
new_df.show()

# COMMAND ----------

sh=df.schema

# COMMAND ----------

df2=[( 15687946,"Osborne",556,"France","Female", 61,2,117419.35,1,1,1,94153.83,0)]

# COMMAND ----------

rdd=spark.sparkContext.parallelize(df2)

# COMMAND ----------

newdf=spark.createDataFrame(rdd,schema=sh)

# COMMAND ----------

newuniondata=df.union(newdf)

# COMMAND ----------

from pyspark.sql.functions import udf

# COMMAND ----------

def power(a):
    return a**2
power(10)
power_udf = udf(power)

# COMMAND ----------

df.select(power_udf(df["Tenure"])).show()

# COMMAND ----------

from pyspark.sql.functions import max,min,mean,corr,stddev,kurtosis,skewness
df.select(max("CreditScore"),stddev("EstimatedSalary")).show()


# COMMAND ----------

df_zomato = spark.read.csv('/FileStore/tables/zomato.csv',sep=",",header=True,inferSchema=True)
df_cc = spark.read.csv('/FileStore/tables/CountryCode.csv',sep=",",header=True,inferSchema=True)


# COMMAND ----------

expression=df_zomato["cc"]==df_cc["cc"]
join_type="inner"
data= df_cc.join(df_zomato,dexpression,join_type)

# COMMAND ----------

data= df_cc.join(df_zomato,df_zomato["cc"]==df_cc["cc"],"inner")

# COMMAND ----------

data.show(5)

# COMMAND ----------

data.columns

# COMMAND ----------

df_zomato.count()

# COMMAND ----------

df_cc.count()

# COMMAND ----------

data.count()

# COMMAND ----------

data= df_cc.join(df_zomato,df_zomato["cc"]==df_cc["cc"],"inner").drop(df_cc.cc).show(6)

# COMMAND ----------

df_apple = spark.read.csv('/FileStore/tables/appl_stock.csv',sep=",",header=True,inferSchema=True)

# COMMAND ----------

df_apple.show(4)

# COMMAND ----------

df_apple.printSchema()

# COMMAND ----------

from pyspark.sql.functions import format_number
df_apple.select(df_apple["Date"],
                format_number(df_apple["High"].cast("float"),2).alias("HIGH"),
                 format_number(df_apple["Open"].cast("float"),2).alias("OPEN"),
                 format_number(df_apple["Low"].cast("float"),2).alias("LOW"),
                 format_number(df_apple["Close"].cast("float"),2).alias("CLOSE")
               )

# COMMAND ----------

from pyspark.sql.functions import hours,dayofmonth,dayofweek,dayofyear,year,date_format,weekofyear,month

# COMMAND ----------

df_apple.select(dayofmonth(df_apple['Date'])).show()

# COMMAND ----------

df_apple.withColumn("year",year(df_apple['Date'])).show()

# COMMAND ----------

df_apple.groupBy("year").sum("High").show()

# COMMAND ----------

FileStore/tables/sales_data-1.csv