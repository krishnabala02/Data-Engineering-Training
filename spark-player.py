# Databricks notebook source
from pyspark.sql import SparkSession
#/FileStore/tables/wc2018_players.csv

# COMMAND ----------

spark=SparkSession.builder.master("local").appName("Master").getOrCreate()

# COMMAND ----------

df = spark.read.csv('/FileStore/tables/wc2018_players.csv',sep=",",header=True,inferSchema=True)
df1=spark.read.csv('/FileStore/tables/wc2018_players.csv',sep=",",header=True,inferSchema=True)

# COMMAND ----------

df1=df1.withColumn("new_height",df1["Height"]+1)
df1.select("FIFA Popular Name","new_height").show(4)

# COMMAND ----------

df.show(4)

# COMMAND ----------

df.select("FIFA Popular Name").show(4)

# COMMAND ----------

df.select("FIFA Popular Name").filter(df["Height"]>170).show()

# COMMAND ----------

from pyspark.sql.functions import when,col
conditions= when(col("Height")>170,"1").when(col("Height")<170,"0")
df3=df.withColumn("less_or_more",conditions)
df3.select("less_or_more","FIFA Popular Name").show(5)

# COMMAND ----------

df.agg({"Height":"min"})

# COMMAND ----------

df.orderBy("Height").select("FIFA Popular Name").show(1)

# COMMAND ----------

df.agg({"Height":"max"}).show()

# COMMAND ----------

df.orderBy("Height").select("FIFA Popular Name").tail(1)

# COMMAND ----------

df.groupBy("Team").count().show()

# COMMAND ----------

df.groupBy("Team").avg().show()

# COMMAND ----------

df=df.groupBy("Team").avg()
df.select("avg(Height)").where(df["Team"] =="Argentina").show()

# COMMAND ----------

 #/FileStore/tables/movies.csv
  #/FileStore/tables/ratings.csv
df_movies = spark.read.csv('/FileStore/tables/movies.csv',sep=",",header=True,inferSchema=True)
df_ratings = spark.read.csv('/FileStore/tables/ratings.csv',sep=",",header=True,inferSchema=True)


# COMMAND ----------

df_movies.show(5)
df_ratings.show(5)

# COMMAND ----------

#Find the list of oldest released movies
df_movies.createOrReplaceTempView('movies')
df_ratings.createOrReplaceTempView('ratings')


# COMMAND ----------

df_movies.printSchema()
df_ratings.printSchema()

# COMMAND ----------

data_full=df_movies.join(df_ratings,on ="movieId",how="outer")
#data_full.show()
data_full.createOrReplaceTempView('moviedetails')

# COMMAND ----------

from pyspark.sql.functions import split

# COMMAND ----------

spark.sql("select * from movies limit 10").show()


# COMMAND ----------

spark.sql("""select substring(title,-5,4) as year from movies limit 5""").show()

# COMMAND ----------

spark.sql("""select substring(trim(title),-5,4) as year from movies limit 5""").show()

# COMMAND ----------

#How many number of movies are there for each rating?
spark.sql("select count(movieId),rating from ratings group by rating").show()


# COMMAND ----------

#How many users have rated each movie?
spark.sql("select distinct(count(userId)),movieId from ratings group by movieId").show()

# COMMAND ----------

spark.sql("select * from moviedetails limit 10").show()

# COMMAND ----------

#What is the total rating for each movie?
spark.sql("select count(rating),title from moviedetails group by title").show()

# COMMAND ----------

#What is the average rating for each movie?
spark.sql("select avg(rating),title from moviedetails group by title").show()

# COMMAND ----------

spark.sql("select * from movies limit 10").show()

# COMMAND ----------

spark.sql("select * from movies where title like(title,'^[0-9]*$')").show()

# COMMAND ----------

spark.sql("select regexp_extract('Toy Story (1995)','(\\d+)',3)").show()

# COMMAND ----------

spark.sql(r"select title,regexp_extract(title,'(\\d+)') as year from movies").show()

# COMMAND ----------

spark.sql(r"select title,regexp_extract(title,'(\\d+)') as year from movies").show()

# COMMAND ----------

spark.sql(r"select title,regexp_extract(title,'(\\w+)(\\d+)',0) as year from movies order by year").show()

# COMMAND ----------

spark.sql(r"select title,regexp_extract(title,'(\(\\d+\))') as year from movies order by year").collect()

# COMMAND ----------

spark.sql("select count(*) from movies").show()

# COMMAND ----------

spark.sql("select regexp_extract(substring(title,-5,4),'(\\d+)') as year from movies").show(9125)

# COMMAND ----------

spark.sql("select substring(title,-5,4) as year from movies order by year ").show()

# COMMAND ----------

spark.sql(r"select title,regexp_extract(title,r'\((\d+)\)'') as year from movies order by year").collect()

# COMMAND ----------

spark.sql("select *, regexp_extract(title,  r'\((\d+)\)', 1) as year from moviedetails limit 3").show(2)

# COMMAND ----------

#Find the list of  oldest released movies.
#How many movies are released each year?
spark.sql("create table moviewithyear as (select *, regexp_extract(title,  r'\((\d+)\)', 1) as year from moviedetails)")

# COMMAND ----------

#How many movies are released each year?
spark.sql("select count(*),year from moviewithyear group by year").show()

# COMMAND ----------

#Find the list of  oldest released movies.
spark.sql("select title,year from moviewithyear where year >1900 & year not null").show()


# COMMAND ----------

spark.sql("select year,title from moviewithyear where year!=null and year >1900").show()

# COMMAND ----------

spark.sql("create table moviewithyear2 as (select *, regexp_extract(title,  r'\(([0-9]{4})', 1) as year from moviedetails)")

# COMMAND ----------

#Find the list of  oldest released movies.
spark.sql("select title,year from moviewithyear2 order by year").show()

# COMMAND ----------

