import pandas as pd
import numpy as np
df1 = pd.read_csv("/Users/purnaraghavaraokalva/Desktop/insurance.csv")
#print(df1)
#print(df1.pivot_table(index='sex', columns='region', values='bmi', aggfunc=np.mean))
#print(df1.pivot_table(index=['sex','age'], columns='region', values='charges', aggfunc=np.max))
#print(df1.pivot_table(index=['sex'], columns='region', values='charges', aggfunc=["mean","std"]))
#print(df1.pivot_table(index=['sex'], columns='region', values='charges', aggfunc=["mean","std"]))
#print(df1.pivot_table(index=['sex'], columns='region', values='charges', aggfunc=["cumsum"]))
#print(df1.pivot(index=['charges'.], columns='age'))
dates=["2017-05-30", "jan 16 2020", "08/09/2020"]
data = pd.to_datetime(dates)
print(data)
date=pd.date_range()


