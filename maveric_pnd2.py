import pandas as pd

data = {
    "calories": [420, 380, 390],
    "duration": [50, 40, 45]
}
df = pd.DataFrame(data)
print(df)
print(df.shape)
print(df.info())
print(df.head(2))
print(df.tail(1))
print(df[["calories", "duration"]].head(2))
print(df["calories"].nunique())
print(df["calories"].unique())
print(df["calories"df.drop(["calories", "duration"], axis=1, inplace=True)
df.drop(0, axis=0, inplace=True)
].value_counts())
