# Databricks notebook source
from pyspark.sql import SparkSession


# COMMAND ----------

spark=SparkSession.builder.master("local").appName("Master").getOrCreate()

# COMMAND ----------

df = spark.read.csv('/FileStore/tables/sales_data-1.csv',sep=",",header=True,inferSchema=True)

# COMMAND ----------

df.show(4)

# COMMAND ----------

from pyspark.sql.functions import hour,dayofmonth,dayofweek,dayofyear,year,date_format,weekofyear,month

# COMMAND ----------

df.select(dayofmonth(df['Order Date'])).show()

# COMMAND ----------

df.columns

# COMMAND ----------

df1 = df.withColumn("total_sales",df['Quantity Ordered']*df['Price Each'])
df1.show()

# COMMAND ----------

df1.groupBy("Month").sum("total_sales").rdd.max()[0]

# COMMAND ----------

#What was the best month for sales? How much was earned that month?
df1.groupBy("Month").sum("total_sales").orderBy("sum(total_sales)", ascending = False).select("Month").show(1)

# COMMAND ----------

#What city sold the most product?
df1.groupBy("City").sum("Quantity Ordered").show()
#df1.groupBy("City").agg(f.max(sum("Quantity Ordered"))).show()

# COMMAND ----------

 #What product sold the most? Why do you think it sold the most
df1.groupBy("Product").sum("Quantity Ordered").show()



# COMMAND ----------

df1.printSchema()

# COMMAND ----------

spark.sql("set spar.sql.legacy.timeParserPolicy=LEGACY")

# COMMAND ----------

spark.sql("set spar.sql.legacy.timeParserPolicy=LEGACY")
from pyspark.sql.functions import to_timestamp
df1=df1.withColumn("Date", to_timestamp("Order Date", 'MM/dd/yy HH:mm')).show(4)
#df1.withColumn("Order_Date" ,to_timestamp(df1["Order Date"], 'MM/dd/yy HH:mm')).show()
#df1.select("Order Date",to_timestamp("Order Date","MM/dd/yy HH:mm").alias("to_orderdate")).show(5)

# COMMAND ----------

#What products are most often sold together?
#df1.groupBy("Order ID").select("Product").flatMap(lambda x: ",".join(x))

# COMMAND ----------


df1.withColumn("products_together", df1.groupBY("Order ID").["Product"].map(lambda x: ",".join(x))

# COMMAND ----------

df2 = df1.dropDuplicates(["Order ID"])
df2.show()

# COMMAND ----------

df3 = df2.groupby("Order ID").agg(collect_list('Product').alias('product_together'))
df3.show()

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import array_join,collect_list
window = Window.partitionBy("Order ID")
df2.withColumn("Product_together", array_join(reverse(collect_list("Product").over(window)), ",")).show(6)
#df.cube("x").count().show()
df2.printSchema()
                            

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import array_join,collect_list
window = Window.partitionBy("Order ID")
df3=df2.withColumn("Product_together", array_join(reverse(collect_list("Product").over(window)), ","))
df3.cube("Product_together").count().show()
df3.printSchema()
              

# COMMAND ----------

from pyspark.sql.functions import array_join,collect_list,aggregate
d_c = df1.groupBy("Order ID").aggregate(collect_list("Product").as("Product_together"))
d_c.printSchema()
d_c.show(false)

# COMMAND ----------

FileStore/tables/gdp.csv
/FileStore/tables/combined_cycle_power_plant.csv