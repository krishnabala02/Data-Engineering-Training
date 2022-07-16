import pandas as pd

df = pd.read_excel("inventory.xlsx")
df1 = pd.read_table("data.csv", delimiter=',')
df2 = df1.sort_values(by='Duration')
df3 = df1.append(df2)
print(df3)
x