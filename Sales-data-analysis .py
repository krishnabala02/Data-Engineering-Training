# Databricks notebook source
import pandas as pd

# COMMAND ----------

all_data=pd.read_csv("E:\Machine Learning\AI_ML_DS_TEST\sales-data.csv")


# COMMAND ----------

# MAGIC %md
# MAGIC What was the best month for sales? How much was earned that month?
# MAGIC What city sold the most product?
# MAGIC What time should we display advertisemens to maximize the likelihood of customer’s buying product?
# MAGIC What products are most often sold together?
# MAGIC What product sold the most? Why do you think it sold the most?

# COMMAND ----------

all_data.shape

# COMMAND ----------

all_data.head()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Question 1: What was the best month for sales? How much was earned that month? 

# COMMAND ----------

all_data['Sales'] = all_data['Quantity Ordered'].astype('int') * all_data['Price Each'].astype('float')

# COMMAND ----------

all_data.head()

# COMMAND ----------

g=all_data.groupby('Month')

# COMMAND ----------

g['Sales'].sum()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Question 2: What city sold the most product?

# COMMAND ----------

g=all_data.groupby('City')

# COMMAND ----------

g['Quantity Ordered'].sum()

# COMMAND ----------

# MAGIC %md
# MAGIC #### What product sold the most? Why do you think it sold the most?

# COMMAND ----------

product_group = all_data.groupby('Product')
quantity_ordered = product_group.sum()['Quantity Ordered']


# COMMAND ----------

quantity_ordered.max()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Question 3: What time should we display advertisements to maximize likelihood of customer's buying product?

# COMMAND ----------

all_data['Hour'] = pd.to_datetime(all_data['Order Date']).dt.hour
#all_data['Minute'] = pd.to_datetime(all_data['Order Date']).dt.minute


# COMMAND ----------

all_data.head()

# COMMAND ----------

g=all_data.groupby('Hour')


# COMMAND ----------

g['Sales'].sum()

# COMMAND ----------

# MAGIC %md
# MAGIC Before 7 sales is low so, we should display advertisements to maximize likelihood of customer's buying product

# COMMAND ----------

all_data['Hour'].value_counts()

# COMMAND ----------

# MAGIC %md
# MAGIC What products are most often sold together?

# COMMAND ----------

all_data[all_data['Order ID']==160873]

# COMMAND ----------

df = all_data[all_data['Order ID'].duplicated(keep=False)]


# COMMAND ----------

item=[]
for i in df['Order ID'].unique():
    s=tuple(df['Product'][df['Order ID']==i])
   # print(s)
    item.append(s)
item

# COMMAND ----------

item_ser=pd.Series(item)


# COMMAND ----------

item_ser.value_counts()

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------

arrayStructData = Seq(
    Row("James", "Java"), Row("James", "C#"),Row("James", "Python"),
    Row("Michael", "Java"),Row("Michael", "PHP"),Row("Michael", "PHP"),
    Row("Robert", "Java"),Row("Robert", "Java"),Row("Robert", "Java"),
    Row("Washington", null)
  )
arrayStructSchema = new StructType().add("name", StringType).add("booksInterested", StringType)
df = spark.createDataFrame(
 spark.sparkContext.parallelize(arrayStructData),arrayStructSchema)
  df.printSchema()
  df.show(false)

  val df2 = df.groupBy("name").agg(collect_list("booksIntersted")
    .as("booksInterested"))
  df2.printSchema()
  df2.show(false)

# COMMAND ----------

