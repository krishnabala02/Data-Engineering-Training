import pandas as pd

df1 = pd.DataFrame({
    "age": [4, 5, 6, 7],
    "sex": ["female", "male", "male", "male"],
    "bmi": [27, 31, 42, 46],
    "children": [0, 5, 4, 7],
    "smoker": ["yes", "no", "yes", "no"],
    "region": ["northeast", "northwest", "north", "south"],
    "charges": [4556, 4550, 7888, 5666],
    "id": [1, 2, 3, 4]
})

df2 = pd.DataFrame({
    "id": [1, 2, 3, 4, 5],
    "condition": ["high", "low", "low", "high", "low"]
})
condition = ["high", "low", "low", "high"]
df3 = pd.DataFrame({
    "problem": ["exe", "jand", "liv", "heart"]
})

# df1.append(df2)
# print(df1.shape)
# print(df2.shape)
# df3 = pd.merge(df1, df2, on="id", how="inner")
# df4 = pd.merge(df1, df2, on="id", how="outer")
# df5 = pd.merge(df1, df2, on="id", how="left")
# df6 = df1.append(df2, ignore_index=True)
# print(df3.shape)
# print(df4.shape)
# print(df5.shape)
# print(df6.shape)
# print(df1.sample(3))
df1.insert(2, "condition", ["high", "low", "high", "low"], True)
print(df1)
df1.insert(4, "problem", df3)
print(df1)
