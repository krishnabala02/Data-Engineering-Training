import pandas as pd

df = pd.read_json('data.json')
x = df["Duration"].mean()
y = df["Pulse"].mean()
df["Duration"].fillna(x, inplace=True)
df["Pulse"].fillna(y, inplace=True)
#df["Calories"].dropna(inplace=True)
#df["Maxpulse"] = pd.to_datetime(df["Maxpulse"])
df1 = df.to_csv('data.csv')

