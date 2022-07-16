import pandas as pd

df = pd.read_json("data.json")
'''print(df.head(3))
print(df.tail(3))
print(df.info())'''
x = df['Duration'].mean()
#y = df['Pulse'].mod()[0]
df['Duration'].fillna('60', inplace =True)
df['Pulse'].fillna('109', inplace =True)
df1 = df['Duration'].fillna(x)
#df2 = df1['Duration'].fillna(y)
print(df1)



