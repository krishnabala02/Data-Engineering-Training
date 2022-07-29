# Databricks notebook source
# MAGIC %md
# MAGIC ![Spark Image](https://upload.wikimedia.org/wikipedia/commons/thumb/f/f3/Apache_Spark_logo.svg/1200px-Apache_Spark_logo.svg.png)

# COMMAND ----------

# MAGIC %md
# MAGIC # Structured APIs

# COMMAND ----------

# MAGIC %md
# MAGIC ## RDDs vs DataFrames and Datasets
# MAGIC 
# MAGIC ![](https://databricks.com/wp-content/uploads/2018/05/rdd-1024x595.png)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Resilient Distributed Dataset (RDD)
# MAGIC RDD was the primary user-facing API in Spark since its inception. At the core, an RDD is an immutable distributed collection of elements of your data, partitioned across nodes in your cluster that can be operated in parallel with a low-level API that offers transformations and actions.
# MAGIC 
# MAGIC ### When to use RDDs?
# MAGIC Consider these scenarios or common use cases for using RDDs when:
# MAGIC - you want low-level transformation and actions and control on your dataset;
# MAGIC - your data is unstructured, such as media streams or streams of text;
# MAGIC - you want to manipulate your data with functional programming constructs than domain specific expressions;
# MAGIC - you don’t care about imposing a schema, such as columnar format, while processing or accessing data attributes by name or column; and
# MAGIC - you can forgo some optimization and performance benefits available with DataFrames and Datasets for structured and semi-structured data.
# MAGIC 
# MAGIC ### What happens to RDDs in Apache Spark 2.0?
# MAGIC You may ask: Are RDDs being relegated as second class citizens? Are they being deprecated?
# MAGIC 
# MAGIC **The answer is a resounding NO!**
# MAGIC 
# MAGIC What’s more, as you will note below, you can seamlessly move between DataFrame or Dataset and RDDs at will—by simple API method calls—and DataFrames and Datasets are built on top of RDDs.

# COMMAND ----------

# MAGIC %md
# MAGIC ### DataFrames
# MAGIC 
# MAGIC Like an RDD, a DataFrame is an immutable distributed collection of data. Unlike an RDD, data is organized into named columns, like a table in a relational database. Designed to make large data sets processing even easier, DataFrame allows developers to impose a structure onto a distributed collection of data, allowing higher-level abstraction; it provides a domain specific language API to manipulate your distributed data; and makes Spark accessible to a wider audience, beyond specialized data engineers.
# MAGIC 
# MAGIC In Spark 2.0, DataFrame APIs will merge with Datasets APIs, unifying data processing capabilities across libraries. Because of this unification, developers now have fewer concepts to learn or remember, and work with a single high-level and type-safe API called `Dataset`.
# MAGIC 
# MAGIC ![Spark](https://databricks.com/wp-content/uploads/2016/06/Unified-Apache-Spark-2.0-API-1.png)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Datasets
# MAGIC Starting in Spark 2.0, Dataset takes on two distinct APIs characteristics: a strongly-typed API and an untyped API, as shown in the table below. Conceptually, consider DataFrame as an alias for a collection of generic objects Dataset[Row], where a Row is a generic untyped JVM object. Dataset, by contrast, is a collection of strongly-typed JVM objects, dictated by a case class you define in Scala or a class in Java.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Typed and Un-typed APIs
# MAGIC 
# MAGIC <table class="table">
# MAGIC <thead>
# MAGIC <tr>
# MAGIC <th>Language</th>
# MAGIC <th>Main Abstraction</th>
# MAGIC </tr>
# MAGIC </thead>
# MAGIC <tbody>
# MAGIC <tr>
# MAGIC <td>Scala</td>
# MAGIC <td>Dataset[T] &amp; DataFrame (alias for Dataset[Row])</td>
# MAGIC </tr>
# MAGIC <tr>
# MAGIC <td>Java</td>
# MAGIC <td>Dataset[T]</td>
# MAGIC </tr>
# MAGIC <tr>
# MAGIC <td>Python*</td>
# MAGIC <td>DataFrame</td>
# MAGIC </tr>
# MAGIC <tr>
# MAGIC <td>R*</td>
# MAGIC <td>DataFrame</td>
# MAGIC </tr>
# MAGIC </tbody>
# MAGIC </table>
# MAGIC 
# MAGIC > **Note:** *Since Python and R have no compile-time type-safety, we only have untyped APIs, namely DataFrames.*

# COMMAND ----------

# MAGIC %md
# MAGIC ### Benefits of Dataset APIs
# MAGIC 
# MAGIC ### 1. Static-typing and runtime type-safety
# MAGIC 
# MAGIC Consider static-typing and runtime safety as a spectrum, with SQL least restrictive to Dataset most restrictive. For instance, in your Spark SQL string queries, you won’t know a syntax error until runtime (which could be costly), whereas in DataFrames and Datasets you can catch errors at compile time (which saves developer-time and costs). That is, if you invoke a function in DataFrame that is not part of the API, the compiler will catch it. However, it won’t detect a non-existing column name until runtime.
# MAGIC 
# MAGIC At the far end of the spectrum is Dataset, most restrictive. Since Dataset APIs are all expressed as lambda functions and JVM typed objects, any mismatch of typed-parameters will be detected at compile time. Also, your analysis error can be detected at compile time too, when using Datasets, hence saving developer-time and costs.
# MAGIC 
# MAGIC All this translates to is a spectrum of type-safety along syntax and analysis error in your Spark code, with Datasets as most restrictive yet productive for a developer.
# MAGIC 
# MAGIC ![](https://databricks.com/wp-content/uploads/2016/07/sql-vs-dataframes-vs-datasets-type-safety-spectrum.png)
# MAGIC 
# MAGIC ### 2. High-level abstraction and custom view into structured and semi-structured data
# MAGIC DataFrames as a collection of Datasets[Row] render a structured custom view into your semi-structured data.
# MAGIC 
# MAGIC ### 3. Ease-of-use of APIs with structure
# MAGIC 
# MAGIC Although structure may limit control in what your Spark program can do with data, it introduces rich semantics and an easy set of domain specific operations that can be expressed as high-level constructs. Most computations, however, can be accomplished with Dataset’s high-level APIs. For example, it’s much simpler to perform `agg`, `select`, `sum`, `avg`, `map`, `filter`, or `groupBy` operations. 
# MAGIC 
# MAGIC ### 4. Performance and Optimization
# MAGIC Along with all the above benefits, you cannot overlook the space efficiency and performance gains in using DataFrames and Dataset APIs for two reasons.
# MAGIC 
# MAGIC First, because DataFrame and Dataset APIs are built on top of the Spark SQL engine, it uses Catalyst to generate an optimized logical and physical query plan. Across R, Java, Scala, or Python DataFrame/Dataset APIs, all relation type queries undergo the same code optimizer, providing the space and speed efficiency. Whereas the Dataset[T] typed API is optimized for data engineering tasks, the untyped Dataset[Row] (an alias of DataFrame) is even faster and suitable for interactive analysis.
# MAGIC 
# MAGIC ![](https://databricks.com/wp-content/uploads/2016/07/memory-usage-when-caching-datasets-vs-rdds.png)
# MAGIC 
# MAGIC Second, since Spark as a compiler understands your Dataset type JVM object, it maps your type-specific JVM object to Tungsten’s internal memory representation using Encoders. As a result, Tungsten Encoders can efficiently serialize/deserialize JVM objects as well as generate compact bytecode that can execute at superior speeds.
# MAGIC 
# MAGIC ### When should I use DataFrames or Datasets?
# MAGIC - If you want rich semantics, high-level abstractions, and domain specific APIs, use DataFrame or Dataset.
# MAGIC - If your processing demands high-level expressions, filters, maps, aggregation, averages, sum, SQL queries, columnar access and use of lambda functions on semi-structured data, use DataFrame or Dataset.
# MAGIC - If you want higher degree of type-safety at compile time, want typed JVM objects, take advantage of Catalyst optimization, and benefit from Tungsten’s efficient code generation, use Dataset.
# MAGIC - If you want unification and simplification of APIs across Spark Libraries, use DataFrame or Dataset.
# MAGIC - If you are a R user, use DataFrames.
# MAGIC - If you are a Python user, use DataFrames and resort back to RDDs if you need more control.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Starting a Spark Session

# COMMAND ----------



# COMMAND ----------

from pyspark.sql import SparkSession
# May take a little while on a local computer
spark = SparkSession.builder.appName("Structured API").getOrCreate()

# COMMAND ----------

cd data

# COMMAND ----------

df = spark.read.format("json").load("/FileStore/tables/2015_summary-2.json")

# COMMAND ----------

df.printSchema()

# COMMAND ----------

spark.read.format("json").load("/FileStore/tables/2015_summary.json").schema

# COMMAND ----------

# MAGIC %md
# MAGIC *A schema is a StructType made up of a number of fields, StructFields, that have a name, type, a Boolean flag which specifies whether that column can contain missing or null values, and, finally, users can optionally specify associated metadata with that column. The metadata is a way of storing information about this column.*
# MAGIC 
# MAGIC *If the types in the data (at runtime) do not match the schema, Spark will throw an error. The example that follows shows how to create and enforce a specific schema on a DataFrame.*

# COMMAND ----------

from pyspark.sql.types import StructField, StructType, StringType, LongType

# COMMAND ----------

myManualSchema = StructType([StructField("DEST_COUNTRY_NAME", StringType(), True),
                             StructField("ORIGIN_COUNTRY_NAME", StringType(), True),
                             StructField("count", LongType(), False, metadata={"hello":"world"}) 
                            ])
df = spark.read.format("json").schema(myManualSchema).load("/FileStore/tables/2015_summary.json")

# COMMAND ----------

df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC **Spark Types**
# MAGIC 
# MAGIC <table class="table">
# MAGIC <tbody><tr>
# MAGIC   <th style="width:20%">Data type</th>
# MAGIC   <th style="width:40%">Value type in Python</th>
# MAGIC   <th>API to access or create a data type</th></tr>
# MAGIC <tr>
# MAGIC   <td> <b>ByteType</b> </td>
# MAGIC   <td>
# MAGIC   int or long <br>
# MAGIC   <b>Note:</b> Numbers will be converted to 1-byte signed integer numbers at runtime.
# MAGIC   Please make sure that numbers are within the range of -128 to 127.
# MAGIC   </td>
# MAGIC   <td>
# MAGIC   ByteType()
# MAGIC   </td>
# MAGIC </tr>
# MAGIC <tr>
# MAGIC   <td> <b>ShortType</b> </td>
# MAGIC   <td>
# MAGIC   int or long <br>
# MAGIC   <b>Note:</b> Numbers will be converted to 2-byte signed integer numbers at runtime.
# MAGIC   Please make sure that numbers are within the range of -32768 to 32767.
# MAGIC   </td>
# MAGIC   <td>
# MAGIC   ShortType()
# MAGIC   </td>
# MAGIC </tr>
# MAGIC <tr>
# MAGIC   <td> <b>IntegerType</b> </td>
# MAGIC   <td> int or long </td>
# MAGIC   <td>
# MAGIC   IntegerType()
# MAGIC   </td>
# MAGIC </tr>
# MAGIC <tr>
# MAGIC   <td> <b>LongType</b> </td>
# MAGIC   <td>
# MAGIC   long <br>
# MAGIC   <b>Note:</b> Numbers will be converted to 8-byte signed integer numbers at runtime.
# MAGIC   Please make sure that numbers are within the range of
# MAGIC   -9223372036854775808 to 9223372036854775807.
# MAGIC   Otherwise, please convert data to decimal.Decimal and use DecimalType.
# MAGIC   </td>
# MAGIC   <td>
# MAGIC   LongType()
# MAGIC   </td>
# MAGIC </tr>
# MAGIC <tr>
# MAGIC   <td> <b>FloatType</b> </td>
# MAGIC   <td>
# MAGIC   float <br>
# MAGIC   <b>Note:</b> Numbers will be converted to 4-byte single-precision floating
# MAGIC   point numbers at runtime.
# MAGIC   </td>
# MAGIC   <td>
# MAGIC   FloatType()
# MAGIC   </td>
# MAGIC </tr>
# MAGIC <tr>
# MAGIC   <td> <b>DoubleType</b> </td>
# MAGIC   <td> float </td>
# MAGIC   <td>
# MAGIC   DoubleType()
# MAGIC   </td>
# MAGIC </tr>
# MAGIC <tr>
# MAGIC   <td> <b>DecimalType</b> </td>
# MAGIC   <td> decimal.Decimal </td>
# MAGIC   <td>
# MAGIC   DecimalType()
# MAGIC   </td>
# MAGIC </tr>
# MAGIC <tr>
# MAGIC   <td> <b>StringType</b> </td>
# MAGIC   <td> string </td>
# MAGIC   <td>
# MAGIC   StringType()
# MAGIC   </td>
# MAGIC </tr>
# MAGIC <tr>
# MAGIC   <td> <b>BinaryType</b> </td>
# MAGIC   <td> bytearray </td>
# MAGIC   <td>
# MAGIC   BinaryType()
# MAGIC   </td>
# MAGIC </tr>
# MAGIC <tr>
# MAGIC   <td> <b>BooleanType</b> </td>
# MAGIC   <td> bool </td>
# MAGIC   <td>
# MAGIC   BooleanType()
# MAGIC   </td>
# MAGIC </tr>
# MAGIC <tr>
# MAGIC   <td> <b>TimestampType</b> </td>
# MAGIC   <td> datetime.datetime </td>
# MAGIC   <td>
# MAGIC   TimestampType()
# MAGIC   </td>
# MAGIC </tr>
# MAGIC <tr>
# MAGIC   <td> <b>DateType</b> </td>
# MAGIC   <td> datetime.date </td>
# MAGIC   <td>
# MAGIC   DateType()
# MAGIC   </td>
# MAGIC </tr>
# MAGIC <tr>
# MAGIC   <td> <b>ArrayType</b> </td>
# MAGIC   <td> list, tuple, or array </td>
# MAGIC   <td>
# MAGIC   ArrayType(<i>elementType</i>, [<i>containsNull</i>])<br>
# MAGIC   <b>Note:</b> The default value of <i>containsNull</i> is <i>True</i>.
# MAGIC   </td>
# MAGIC </tr>
# MAGIC <tr>
# MAGIC   <td> <b>MapType</b> </td>
# MAGIC   <td> dict </td>
# MAGIC   <td>
# MAGIC   MapType(<i>keyType</i>, <i>valueType</i>, [<i>valueContainsNull</i>])<br>
# MAGIC   <b>Note:</b> The default value of <i>valueContainsNull</i> is <i>True</i>.
# MAGIC   </td>
# MAGIC </tr>
# MAGIC <tr>
# MAGIC   <td> <b>StructType</b> </td>
# MAGIC   <td> list or tuple </td>
# MAGIC   <td>
# MAGIC   StructType(<i>fields</i>)<br>
# MAGIC   <b>Note:</b> <i>fields</i> is a Seq of StructFields. Also, two fields with the same
# MAGIC   name are not allowed.
# MAGIC   </td>
# MAGIC </tr>
# MAGIC <tr>
# MAGIC   <td> <b>StructField</b> </td>
# MAGIC   <td> The value type in Python of the data type of this field
# MAGIC   (For example, Int for a StructField with the data type IntegerType) </td>
# MAGIC   <td>
# MAGIC   StructField(<i>name</i>, <i>dataType</i>, [<i>nullable</i>])<br>
# MAGIC   <b>Note:</b> The default value of <i>nullable</i> is <i>True</i>.
# MAGIC   </td>
# MAGIC </tr>
# MAGIC </tbody></table>

# COMMAND ----------

df.show()

# COMMAND ----------

df.first()

# COMMAND ----------

# MAGIC %md
# MAGIC *This is a row object in spark DataFrame*

# COMMAND ----------

df.first().asDict()['DEST_COUNTRY_NAME']

# COMMAND ----------

df.take(3)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Columns and Expressions
# MAGIC 
# MAGIC There are a lot of different ways to construct and refer to columns but the two simplest ways are
# MAGIC by using the `col` or `column` functions.

# COMMAND ----------

from pyspark.sql.functions import col, column

# COMMAND ----------

df.select(col("DEST_COUNTRY_NAME")).show(2)

# COMMAND ----------

df.select(column("DEST_COUNTRY_NAME")).show(2)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Expressions

# COMMAND ----------

from pyspark.sql.functions import expr
df.select(expr("DEST_COUNTRY_NAME as Destination")).show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## select and selectExpr

# COMMAND ----------

df.select("DEST_COUNTRY_NAME").show(2)

# COMMAND ----------

df.select("DEST_COUNTRY_NAME", "ORIGIN_COUNTRY_NAME").show(2)


# COMMAND ----------

df.select(expr("DEST_COUNTRY_NAME"),
                col("DEST_COUNTRY_NAME"),
                column("DEST_COUNTRY_NAME")).show(2)

# COMMAND ----------

df.select(expr("DEST_COUNTRY_NAME AS destination")).show(2)

# COMMAND ----------

df.select(expr("DEST_COUNTRY_NAME as destination").alias("DEST_COUNTRY_NAME")).show(2)

# COMMAND ----------

df.selectExpr("DEST_COUNTRY_NAME as newColumnName", "DEST_COUNTRY_NAME").show(2)

# COMMAND ----------

df.selectExpr(
"*", # all original columns
"(DEST_COUNTRY_NAME = ORIGIN_COUNTRY_NAME) as withinCountry").show(2)

# COMMAND ----------

df.selectExpr("avg(count)", "count(distinct(DEST_COUNTRY_NAME))").show(2)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Adding Columns

# COMMAND ----------

# Adding a column with literal 1
from pyspark.sql.functions import lit
df.withColumn("numberOne", lit(1)).show(2)

# COMMAND ----------

df.withColumn("withinCountry", expr("ORIGIN_COUNTRY_NAME == DEST_COUNTRY_NAME")).show(2)

# COMMAND ----------

# Renaming Columns
df.withColumnRenamed("DEST_COUNTRY_NAME", "dest").columns

# COMMAND ----------

df.withColumn("withinCountry", expr("ORIGIN_COUNTRY_NAME == DEST_COUNTRY_NAME")).show(2)

# COMMAND ----------

# Removing Columns
df.drop("ORIGIN_COUNTRY_NAME").columns

# COMMAND ----------

# Changing a Column’s Type (cast)
df.withColumn("count2", col("count").cast("long")).schema

# COMMAND ----------

# MAGIC %md
# MAGIC ## Filtering Rows

# COMMAND ----------

df.filter(col("count") < 2).show(2)

# COMMAND ----------

df.where("count < 2").show(2)

# COMMAND ----------

df.where(col("count") < 2).where(col("ORIGIN_COUNTRY_NAME") != "Croatia").show(2)

# COMMAND ----------

# Getting Unique Rows
df.select("ORIGIN_COUNTRY_NAME", "DEST_COUNTRY_NAME").distinct().count()

# COMMAND ----------

df.select("ORIGIN_COUNTRY_NAME").distinct().count()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Random Samples

# COMMAND ----------

seed = 5
withReplacement = False
fraction = 0.5
df.sample(withReplacement, fraction, seed).count()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Concatenating and Appending Rows (Union)

# COMMAND ----------

from pyspark.sql import Row
schema = df.schema
newRows = [Row("New Country", "Other Country", 5),Row("New Country 2", "Other Country 3", 1)]
parallelizedRows = spark.sparkContext.parallelize(newRows)
newDF = spark.createDataFrame(parallelizedRows, schema)

# COMMAND ----------

newDF.show(10)

# COMMAND ----------

df.show(6)

# COMMAND ----------

df.union(newDF).show()

# COMMAND ----------

df.union(newDF).where("count = 1").where(col("ORIGIN_COUNTRY_NAME") != "United States").show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Sorting Rows

# COMMAND ----------

df.show(5)

# COMMAND ----------

df.sort("count").show(5)

# COMMAND ----------

df.orderBy("count", "DEST_COUNTRY_NAME").show(5)

# COMMAND ----------

df.orderBy(col("count"), col("DEST_COUNTRY_NAME")).show(5)

# COMMAND ----------

# MAGIC %md
# MAGIC *By default it sorts in ascending order but if you want to explicitly define the order use desc and asc*

# COMMAND ----------

from pyspark.sql.functions import desc, asc
df.orderBy(expr("count desc")).show(2)

# COMMAND ----------

df.orderBy(col("count").desc(), col("DEST_COUNTRY_NAME").asc()).show(2)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Limit

# COMMAND ----------

df.orderBy(expr("count desc")).limit(6).show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Repartition and Coalesce

# COMMAND ----------

df.rdd.getNumPartitions()

# COMMAND ----------

df.repartition(5)

# COMMAND ----------

df.repartition(5).rdd.getNumPartitions()

# COMMAND ----------

# If you know that you’re going to be filtering by a certain column often, 
# it can be worth repartitioning based on that column
df.repartition(col("DEST_COUNTRY_NAME"))

# COMMAND ----------

df.repartition(5, col("DEST_COUNTRY_NAME"))

# COMMAND ----------

# MAGIC %md
# MAGIC *Coalesce, on the other hand, will not incur a full shuffle and will try to combine partitions.*

# COMMAND ----------

df.repartition(5, col("DEST_COUNTRY_NAME")).coalesce(2)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Working with Different Types of Data

# COMMAND ----------

df = spark.read.format("csv")\
.option("header", "true")\
.option("inferSchema", "true")\
.load("data/2010-12-01.csv")
df.printSchema()

# COMMAND ----------

from pyspark.sql.functions import lit
df.select(lit(5), lit("five"), lit(5.0))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Working with Booleans

# COMMAND ----------

from pyspark.sql.functions import col
df.where(col("InvoiceNo") != 536365)\
.select("InvoiceNo", "Description")\
.show(5, False)

# COMMAND ----------

from pyspark.sql.functions import instr
priceFilter = col("UnitPrice") > 600
descripFilter = instr(df.Description, "POSTAGE") >= 1
df.where(df.StockCode.isin("DOT")).where(priceFilter | descripFilter).show()

# COMMAND ----------

DOTCodeFilter = col("StockCode") == "DOT"
priceFilter = col("UnitPrice") > 600
descripFilter = instr(col("Description"), "POSTAGE") >= 1
df.withColumn("isExpensive", DOTCodeFilter & (priceFilter | descripFilter))\
.where("isExpensive")\
.select("unitPrice", "isExpensive").show(5)

# COMMAND ----------

df.withColumn("isExpensive", expr("NOT UnitPrice <= 250"))\
.where("isExpensive")\
.select("Description", "UnitPrice").show(5)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Working with Numbers

# COMMAND ----------

from pyspark.sql.functions import expr, pow
fabricatedQuantity = pow(col("Quantity") * col("UnitPrice"), 2) + 5
df.select(expr("CustomerId"), fabricatedQuantity.alias("realQuantity")).show(2)

# COMMAND ----------

df.selectExpr(
"CustomerId",
"(POWER((Quantity * UnitPrice), 2.0) + 5) as realQuantity").show(2)

# COMMAND ----------

from pyspark.sql.functions import lit, round, bround
df.select(round(lit("2.5")), bround(lit("2.5"))).show(2)

# COMMAND ----------

from pyspark.sql.functions import corr
df.stat.corr("Quantity", "UnitPrice")
df.select(corr("Quantity", "UnitPrice")).show()

# COMMAND ----------

df.describe().show()

# COMMAND ----------

from pyspark.sql.functions import count, mean, stddev_pop, min, max
colName = "UnitPrice"
quantileProbs = [0.5]
relError = 0.05
df.stat.approxQuantile("UnitPrice", quantileProbs, relError) # 2.51

# COMMAND ----------

df.stat.crosstab("StockCode", "Quantity").show()

# COMMAND ----------

df.stat.freqItems(["StockCode", "Quantity"]).show()

# COMMAND ----------

from pyspark.sql.functions import monotonically_increasing_id
df.select(monotonically_increasing_id()).show(2)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Working with Strings

# COMMAND ----------

from pyspark.sql.functions import initcap
df.select(initcap(col("Description"))).show()

# COMMAND ----------

from pyspark.sql.functions import lower, upper
df.select(col("Description"),
lower(col("Description")),
upper(lower(col("Description")))).show(2)

# COMMAND ----------

from pyspark.sql.functions import lit, ltrim, rtrim, rpad, lpad, trim
df.select(
ltrim(lit("   HELLO   ")).alias("ltrim"),
rtrim(lit("   HELLO   ")).alias("rtrim"),
trim(lit("    HELLO   ")).alias("trim"),
lpad(lit("HELLO"), 3, " ").alias("lp"),
rpad(lit("HELLO"), 10, " ").alias("rp")).show(2)

# COMMAND ----------

# Regular Expressions
from pyspark.sql.functions import regexp_replace
regex_string = "BLACK|WHITE|RED|GREEN|BLUE"
df.select(
regexp_replace(col("Description"), regex_string, "COLOR").alias("color_clean"),
col("Description")).show(2)


# COMMAND ----------

from pyspark.sql.functions import translate
df.select(translate(col("Description"), "LEET", "1337"),col("Description")).show(2)

# COMMAND ----------

from pyspark.sql.functions import regexp_extract
extract_str = "(BLACK|WHITE|RED|GREEN|BLUE)"
df.select(
regexp_extract(col("Description"), extract_str, 1).alias("color_clean"),
col("Description")).show(2)

# COMMAND ----------

from pyspark.sql.functions import instr
containsBlack = instr(col("Description"), "BLACK") >= 1
containsWhite = instr(col("Description"), "WHITE") >= 1
df.withColumn("hasSimpleColor", containsBlack | containsWhite)\
.where("hasSimpleColor")\
.select("Description").show(3, False)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Working with Dates and Timestamps

# COMMAND ----------

from pyspark.sql.functions import current_date, current_timestamp
dateDF = spark.range(10)\
.withColumn("today", current_date())\
.withColumn("now", current_timestamp())
dateDF.show()

# COMMAND ----------

dateDF.printSchema()

# COMMAND ----------

from pyspark.sql.functions import date_add, date_sub
dateDF.select(date_sub(col("today"), 5), date_add(col("today"), 5)).show(1)

# COMMAND ----------

from pyspark.sql.functions import datediff, months_between, to_date

dateDF.withColumn("week_ago", date_sub(col("today"), 7))\
.select(datediff(col("week_ago"), col("today"))).show(1)

# COMMAND ----------

dateDF.select(
to_date(lit("2016-01-01")).alias("start"),
to_date(lit("2017-05-22")).alias("end"))\
.select(months_between(col("start"), col("end"))).show(1)

# COMMAND ----------

from pyspark.sql.functions import to_date, lit
spark.range(5).withColumn("date", lit("2017-01-01"))\
.select(to_date(col("date"))).show(1)

# COMMAND ----------

from pyspark.sql.functions import to_date
dateFormat = "yyyy-dd-MM"
cleanDateDF = spark.range(1).select(
to_date(lit("2017-12-11"), dateFormat).alias("date"),
to_date(lit("2017-20-12"), dateFormat).alias("date2"))
cleanDateDF.show()

# COMMAND ----------

from pyspark.sql.functions import to_timestamp
cleanDateDF.select(to_timestamp(col("date"), dateFormat)).show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Working with Nulls in Data

# COMMAND ----------

from pyspark.sql.functions import coalesce
df.select(coalesce(col("Description"), col("CustomerId"))).show()

# COMMAND ----------

df.na.drop()

# COMMAND ----------

df.na.drop("any")

# COMMAND ----------

df.na.drop("all")

# COMMAND ----------

df.na.drop("all", subset=["StockCode", "InvoiceNo"])

# COMMAND ----------

df.na.fill("all", subset=["StockCode", "InvoiceNo"])

# COMMAND ----------

df.na.fill("All Null values become this string")

# COMMAND ----------

df.na.replace([""], ["UNKNOWN"], "Description")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Working with Complex Types

# COMMAND ----------

from pyspark.sql.functions import struct
complexDF = df.select(struct("Description", "InvoiceNo").alias("complex"))
complexDF.show()

# COMMAND ----------

from pyspark.sql.functions import split
df.select(split(col("Description"), " ")).show(2)

# COMMAND ----------

df.select(split(col("Description"), " ").alias("array_col"))\
.selectExpr("array_col[0]").show(2)

# COMMAND ----------

from pyspark.sql.functions import size
df.select(size(split(col("Description"), " "))).show(2) # shows 5 and 3

# COMMAND ----------

from pyspark.sql.functions import array_contains
df.select(array_contains(split(col("Description"), " "), "WHITE")).show(2)

# COMMAND ----------

from pyspark.sql.functions import split, explode
df.withColumn("splitted", split(col("Description"), " "))\
.withColumn("exploded", explode(col("splitted")))\
.select("Description", "InvoiceNo", "exploded").show(2)

# COMMAND ----------

# Maps are created by using the map function and key-value pairs of columns
from pyspark.sql.functions import create_map
df.select(create_map(col("Description"), col("InvoiceNo")).alias("complex_map")).show(2)

# COMMAND ----------

df.select(create_map(col("Description"), col("InvoiceNo")).alias("complex_map"))\
.selectExpr("complex_map['WHITE METAL LANTERN']").show(2)

# COMMAND ----------

df.select(create_map(col("Description"), col("InvoiceNo")).alias("complex_map"))\
.selectExpr("explode(complex_map)").show(2)

# COMMAND ----------

# MAGIC %md
# MAGIC ### User-Defined Functions

# COMMAND ----------

udfExampleDF = spark.range(5).toDF("num")

def power3(double_value):
    return double_value ** 3
power3(2.0)

# COMMAND ----------

from pyspark.sql.functions import udf
power3udf = udf(power3)

# COMMAND ----------

udfExampleDF.show()

# COMMAND ----------

from pyspark.sql.functions import col
udfExampleDF.select(power3udf(col("num"))).show()

# COMMAND ----------

from pyspark.sql.types import IntegerType, DoubleType
spark.udf.register("power3py", power3, DoubleType())

# COMMAND ----------

udfExampleDF.selectExpr("power3py(num)").show(2)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Aggregation

# COMMAND ----------

df = spark.read.format("csv")\
.option("header", "true")\
.option("inferSchema", "true")\
.load("data/retail-data/all/*.csv")\
.coalesce(5)
df.cache()

# COMMAND ----------

df.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Aggregation Functions

# COMMAND ----------

from pyspark.sql.functions import count
df.select(count("StockCode")).show()

# COMMAND ----------

from pyspark.sql.functions import countDistinct
df.select(countDistinct("StockCode")).show() 

# COMMAND ----------

from pyspark.sql.functions import approx_count_distinct
df.select(approx_count_distinct("StockCode", 0.1)).show() # 3364

# COMMAND ----------

from pyspark.sql.functions import first, last
df.select(first("StockCode"), last("StockCode")).show()

# COMMAND ----------

from pyspark.sql.functions import min, max
df.select(min("Quantity"), max("Quantity")).show()

# COMMAND ----------

from pyspark.sql.functions import sum
df.select(sum("Quantity")).show()

# COMMAND ----------

from pyspark.sql.functions import sumDistinct
df.select(sumDistinct("Quantity")).show()

# COMMAND ----------

from pyspark.sql.functions import sum, count, avg, expr
df.select(
count("Quantity").alias("total_transactions"),
sum("Quantity").alias("total_purchases"),
avg("Quantity").alias("avg_purchases"),
expr("mean(Quantity)").alias("mean_purchases"))\
.selectExpr(
"total_purchases/total_transactions",
"avg_purchases",
"mean_purchases").show()

# COMMAND ----------

from pyspark.sql.functions import var_pop, stddev_pop
from pyspark.sql.functions import var_samp, stddev_samp
df.select(var_pop("Quantity"), var_samp("Quantity"),
stddev_pop("Quantity"), stddev_samp("Quantity")).show()

# COMMAND ----------

from pyspark.sql.functions import skewness, kurtosis
df.select(skewness("Quantity"), kurtosis("Quantity")).show()

# COMMAND ----------

from pyspark.sql.functions import corr, covar_pop, covar_samp
df.select(corr("InvoiceNo", "Quantity"), covar_samp("InvoiceNo", "Quantity"),
covar_pop("InvoiceNo", "Quantity")).show()

# COMMAND ----------

from pyspark.sql.functions import collect_set, collect_list
df.agg(collect_set("Country"), collect_list("Country")).show()

# COMMAND ----------

df.groupBy("InvoiceNo", "CustomerId").count().show()

# COMMAND ----------

df.groupBy("InvoiceNo").agg(
count("Quantity").alias("quan"),
expr("count(Quantity)")).show()

# COMMAND ----------

df.groupBy("InvoiceNo").agg(expr("avg(Quantity)"),expr("stddev_pop(Quantity)"))\
.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Joins

# COMMAND ----------

person = spark.createDataFrame([
(0, "Bill Chambers", 0, [100]),
(1, "Matei Zaharia", 1, [500, 250, 100]),
(2, "Michael Armbrust", 1, [250, 100])])\
.toDF("id", "name", "graduate_program", "spark_status")
graduateProgram = spark.createDataFrame([
(0, "Masters", "School of Information", "UC Berkeley"),
(2, "Masters", "EECS", "UC Berkeley"),
(1, "Ph.D.", "EECS", "UC Berkeley")])\
.toDF("id", "degree", "department", "school")
sparkStatus = spark.createDataFrame([
(500, "Vice President"),
(250, "PMC Member"),
(100, "Contributor")])\
.toDF("id", "status")

# COMMAND ----------

person.show()

# COMMAND ----------

graduateProgram.show()

# COMMAND ----------

sparkStatus.show()

# COMMAND ----------

joinExpression = person["graduate_program"] == graduateProgram['id']

# COMMAND ----------

wrongJoinExpression = person["name"] == graduateProgram["school"]

# COMMAND ----------

joinType = "inner"
person.join(graduateProgram, joinExpression, joinType).show()

# COMMAND ----------

joinType = "outer"
person.join(graduateProgram, joinExpression, joinType).show()

# COMMAND ----------

joinType = "left_outer"
graduateProgram.join(person, joinExpression, joinType).show()

# COMMAND ----------

joinType = "right_outer"
person.join(graduateProgram, joinExpression, joinType).show()

# COMMAND ----------

joinType = "left_semi"
graduateProgram.join(person, joinExpression, joinType).show()

# COMMAND ----------

gradProgram2 = graduateProgram.union(spark.createDataFrame([
(0, "Masters", "Duplicated Row", "Duplicated School")]))
gradProgram2.show()

# COMMAND ----------

joinType = "left_anti"
graduateProgram.join(person, joinExpression, joinType).show()

# COMMAND ----------

joinType = "cross"
graduateProgram.join(person, joinExpression, joinType).show()

# COMMAND ----------

# MAGIC %md
# MAGIC #  Spark SQL

# COMMAND ----------

# MAGIC %md
# MAGIC To use SQL queries directly with the dataframe, you will need to register it to a temporary view:

# COMMAND ----------

spark.read.json("data/2015-summary.json")\
.createOrReplaceTempView("some_sql_view") # DF => SQL

# COMMAND ----------

spark.sql("""
SELECT DEST_COUNTRY_NAME, sum(count)
FROM some_sql_view GROUP BY DEST_COUNTRY_NAME
""")\
.where("DEST_COUNTRY_NAME like 'S%'").where("`sum(count)` > 10")\
.count() # SQL => DF

# COMMAND ----------

spark.end()
/FileStore/tables/wc2018_players.csv