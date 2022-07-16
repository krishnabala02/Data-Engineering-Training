import pandas as pd
import numpy as np

df = pd.read_csv("/Users/purnaraghavaraokalva/Desktop/maveric_pandas_projec/zomato.csv", on_bad_lines="skip",
                 encoding="latin - 1")
df1 = pd.read_excel("/Users/purnaraghavaraokalva/Desktop/maveric_pandas_projec/Country-Code.xlsx")
#print(df1)
#/Users/purnaraghavaraokalva/Desktop/maveric_pandas_projec/Bank_churn_modelling - Copy.csv
# print(df)
# Which restaurant has the largest number of votes from the customer
#data = df.groupby(["Restaurant Name"])["Votes"].sum().sort_values(ascending=False).head(1)
#print(data)

# Which city is costliest in each country? ( Assume all the currency are of same value)
# g = df.groupby('cc')
# for countrycode, dataframe in g:
# print(countrycode)
# print(dataframe.sort_values(by=["Average Cost for two"]).tail(1))

# Aggregate Rating
# 2)	Which city has more number of poor and not rated rating than very good rating in each country?
# h = df.groupby('cc')
# for countrycode, dataframe in h:
# print(countrycode)
# print(dataframe.sort_values(by=["Aggregate Rating"]).head(1))

# 4) In india how many restaurants are present in each locality?

#df2 = df1.rename(columns={"Country Code": 'cc'})
#df3 = pd.merge(df, df2, on="cc", how="outer")
#df4 = df3.where(df3["Country"] == "India")
#j = df4.groupby('Locality')
#for locality, dataframe in j:
    #print(locality)
    #print(dataframe.count())


#5) Which city has the most number of restaurants in each country?
#k = df.groupby('cc')
#for countrycode, dataframe in k:
    #print(countrycode)
    #print(dataframe.groupby('City').max())

#6) Which franchise has the highest number of Restaurants?
#print(df.groupby('Restaurant Name')["Restaurant ID"].count().sort_values(ascending= False).head(1).head(1))


#7) How many Restaurants are accepting online orders?
#df6 = df.where(df["Has Online delivery"] == "Yes")
#print(df6.size)

#8) How many have a book table facility?
#df7 = df.where(df["Has Table booking"] == "Yes")
#print(df7.size)

#9) Which location has the highest number of Restaurants?
#m = df.groupby('City')
#out ={}
#for city, dataframe in m:
   #out[city] = dataframe.size
#max_val = max(out.values())

#print()
#for key in out.keys():
    #if max_val == out[key]:
        #max_key = key
        #print(key)
#print (f"location :{max_key},  number of restaurants:{max_val}")

#10) How many types of Restaurant types are there?
#m = df.groupby('Restaurant Name')
#out1 =[]
#for Restaurant,dataframe in m:
     #out1.append(Restaurant)

#out2 = tuple(out1)
#count =0
#print(out2)
#for i in out2:
    #count = count + 1

#print(count)

#11) What is the most liked Restaurant type?
#df11 = df.sort_values(by=['Votes'], ascending=False).groupby("Restaurant Name").head(1)
#print(df.groupby("Restaurant Name").apply(lambda x : x.sort_values(["Votes"], ascending=False).head(1)))
#print(df11.head(1))

#12) What is the Average cost for 2 persons in each city?
#data1 = df.groupby(["City"])["Average Cost for two"].sum().sort_values(ascending=False)
#print(data1)

#13) What is the most liked Dish type?
#Aggregate rating
#Cuisines
#df12 = df.sort_values(by=['Aggregate rating'], ascending=False).groupby("Cuisines").head(1)
#print(df12["Cuisines"].head(1))

#8) How many have a book table facility?
#df7 = df.where(df["Has Table booking"] == "Yes")
#print(df7.size)
df7 = df.query('`Has Table booking`=="Yes"')
print(df7.size)












