import pandas as pd
df1 = pd.read_csv("/Users/purnaraghavaraokalva/Downloads/instanceUsage_data.csv",chunksize=100000)
df2 = pd.read_csv("/Users/purnaraghavaraokalva/Desktop/instanceEvent_data.csv")
print(df1.head())
print(df2.head())
df=[]
for i in data:
  print(i.size)
  df.append(i)


