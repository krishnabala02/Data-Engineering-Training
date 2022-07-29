# Databricks notebook source
#GDP DATA

# To create the dataframe in pyspark 

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType

# COMMAND ----------

# Initializing the sparksesstion 

spark = SparkSession \
    .builder \
    .appName("Python Spark SQL basic example") \
    .config("spark.some.config.option", "some-value") \
    .getOrCreate()

# COMMAND ----------

schema = StructType([StructField('_c0', IntegerType(), True), 
                      StructField('Country', StringType(), True), 
                      StructField('Year', IntegerType(), True), 
                      StructField('GDP', FloatType(), True)
                     ])

# COMMAND ----------

# Redefining the dataframe with correct column labels 

df1 = spark.read.csv('country_data.csv', header=True, schema=schema)
df1.show(5)

# COMMAND ----------

# We can do SQL operations on the dataframe by creating 
# a temporary view 

df1.createOrReplaceTempView('GDP')

# COMMAND ----------

# Now pass the SQL command as a string 

# Selecting one column from dataframe 

spark.sql('select Country from GDP').show(5)

# COMMAND ----------

# Selecting multiple columns from dataframe 

spark.sql('select Country, GDP from GDP').show(5)

# COMMAND ----------

# length counts the length of the string 

spark.sql('select Country, length(Country) from GDP').show(5)

# COMMAND ----------

# Selecting first three letters from the country 

spark.sql("select Country, left(Country, 3) from GDP").show(5)

# COMMAND ----------

# applying condition on selection

spark.sql("select Year, GDP from GDP where Country = 'Nepal'").show(5)

# COMMAND ----------

# applying mathematical condition on selection

spark.sql('select Country from GDP where GDP < 10').show(5)

# COMMAND ----------

# Star selects all the columns from the dataframe 

spark.sql("select * from GDP where Year = 2011").show(5)

# COMMAND ----------

# Select multiple column with condition

spark.sql("select Country, Year from GDP where Year = 2000").show(5)

# COMMAND ----------

# Selecting multiple columns with multiple conditions 

spark.sql("select Country, Year, GDP from GDP\
            where Year = 2000 and GDP > 35000").show()

# COMMAND ----------

# Selecting countries whose name starts from Z

spark.sql("select Country from GDP where Country like 'Z%'").show(5)

# COMMAND ----------

# Selecting country not having vowel a in its name and starting with S

spark.sql("select Country from GDP\
            where Country not like '%a%' \
            and Country like 'S%'").show(5)

# COMMAND ----------

# Use of between 

spark.sql("select Year, GDP from GDP \
            where Country = 'China' \
            and Year between 2005 and 2010").show(5)

# COMMAND ----------

# Use of in 

spark.sql("select Country, GDP from GDP \
            where Country in ('Nepal', 'India') \
            and Year = 2000").show(5)

# COMMAND ----------

# Use of in outside condition can be used to create yes no question 

spark.sql("select distinct Country, Country in ('Pakistan', 'India') from GDP").show(4)

# COMMAND ----------

# Use of distinct 

spark.sql("select distinct * from GDP where length(Country) < 5").show(5)

# COMMAND ----------

# A combination for fun 

spark.sql("select distinct * from GDP where Country like '%l'").show(5)

# COMMAND ----------

# Order by 

spark.sql("select * from GDP where Year = 2015 order by GDP desc").show(4)

# COMMAND ----------

# We can create another temp view 

df1.createOrReplaceTempView('Country')

# COMMAND ----------

# We can select from COuntry now  

spark.sql('SELECT DISTINCT Country, GDP FROM Country\
            WHERE GDP < 10').show()

# COMMAND ----------

# GDP is still working 

spark.sql('SELECT Country FROM GDP\
            WHERE Year=2010').show(5)

# COMMAND ----------

# How many country name start with United?

spark.sql("select distinct Country from GDP where Country like 'United%'").show()

# COMMAND ----------

# Subquerry 
# Which country in 2010 had GDP larger than the GDP of Germany in 2015?

spark.sql("select distinct Country from GDP \
          where Year = 2010 and \
          GDP > (select GDP from GDP \
                          where Country = 'Germany'\
                          and Year = '2015')").show()

# COMMAND ----------

# Lowest GDP which is not null 

spark.sql("select * from GDP where GDP > 0 order by GDP asc").show(3)

# COMMAND ----------

# Countries with the shortest name 

spark.sql("select distinct Country, length(Country) from GDP \
            where length(Country)>0 \
            order by length(Country)").show(10) 

# COMMAND ----------

# Longest country name 

spark.sql("select Country, Year from GDP order by length(Country) desc").show(4)

# COMMAND ----------

# Viewing the full name 

spark.sql("select Country, Year from GDP order by length(Country) desc").first()

# COMMAND ----------

# Total GDP of the world in 2000

spark.sql("select sum(GDP) from GDP where Year = 2000").show()

# COMMAND ----------

# Average GDP of Chile 

spark.sql("select mean(GDP) from GDP where Country = 'Chile'").show()

# COMMAND ----------

# Number of countires in the world 

spark.sql("select count(distinct Country) from GDP").show()

# COMMAND ----------

# Total GDP of India and Pakistan in 2000

spark.sql("select sum(GDP) from GDP \
            where Country in ('India', 'Pakistan')\
            and Year = 2000").show()

# COMMAND ----------

# Number of countries with GDP more than 30000 in 2000

spark.sql("select count(Country) from GDP \
            where Year = 2000 \
            and GDP > 30000").show()

# COMMAND ----------

