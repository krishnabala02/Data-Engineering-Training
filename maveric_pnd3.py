import pandas as pd
df = pd.read_parquet("Data_Subsetw.parquet")
#print(df)
#print(df.shape)
#print(df.info())
#print(df.head(2))
#print(df.info())
#print(df.tail(1))
#print(df["wetBulbAvg"].nunique())
#print(df["wetBulbAvg"].unique())
#df.drop(0, axis=0, inplace=True)
print(df["SBU"].nunique())
#print(df.head(4))
print(df["SBU"].value_counts())