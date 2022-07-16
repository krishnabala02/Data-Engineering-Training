
import pandas as pd
from collections import Counter
import numpy as np
df = pd.read_csv("/Users/purnaraghavaraokalva/Desktop/sales-data.csv")
#What was the best month for sales? How much was earned that month?
df["total_sales"] = df["Quantity Ordered"]*df["Price Each"]
g = df.groupby("Month")["total_sales"].sum().agg("max")
i = df.groupby("Month")["total_sales"].sum().idxmax()
print("Best month for sales is")
print(i)
print("total sales amount for best month")
print(g)
#	What city sold the most product?
h = df.groupby("City")["Quantity Ordered"].sum().agg("max")
f = df.groupby("City")["Quantity Ordered"].sum().idxmax()
print("city which sold most products is")
print(f)
'''#	What city sold the most product?
h = df.groupby("City")["total_sales"].sum().agg("max")
f = df.groupby("City")["total_sales"].sum().idxmax()
print("city which sold most products is")
print(f)'''
#•	What time should we display advertisemens to maximize the likelihood of customer’s buying product?
df["Order_Date_value"]=pd.to_datetime(df["Order Date"])
df["hour"]=df["Order Date_value"].dt.hour
print(df)
j = df.groupby("hour")["total_sales"].sum()
h =df.groupby("hour")["total_sales"].sum().max()
print(j)
print(h)
#•	What products are most often sold together?
#this dataframe to seperate duplicated values
df_new=df[df["Order ID"].duplicated(keep=False)]
product_together_list=[]
#df_new=df[df["Quantity Ordered"].duplicated(keep=False)]
'''df = pd.DataFrame({'A': range(3), 'B': range(1, 4)})
df
   A  B
0  0  1
1  1  2
2  2  3
df.transform(lambda x: x + 1)
   A  B
0  1  2
1  2  3
2  3  4'''
df_new["products_together"]=df_new.groupby("Order ID")["Product"].transform(lambda x: ",".join(x))
df_new=df_new[["products_together","Order ID"]].drop_duplicates()
for each_row in df_new["products_together"]:
    product_together_list.append(each_row)

# frequency count of column products_together
count = df_new.groupby('products_together').value_counts()
print(count)
#print(df_new)
#print(df_new.size)

#What product sold the most? Why do you think it sold the most?
m=df.groupby("Product")["Quantity Ordered"].sum().max
p=df.groupby("Product")["Quantity Ordered"].sum().idxmax()
print("product sold most is:")
print(p)



