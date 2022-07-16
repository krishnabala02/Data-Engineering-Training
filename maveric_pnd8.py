import pandas as pd

df = pd.read_csv("insurance.csv")
x = lambda a : a + 10 if a > 20 else a
df["bmi"] = df["bmi"].apply(x)
print(df)