import pandas as pd
from maveric2 import sum
df = pd.read_csv("Bank_churn_modelling - Copy.csv")
for x in df.index:
    #print(df.index)
    if df.loc[x, "CreditScore"] > 500:
        print(df.loc[x, "CreditScore"])


print(sum(2, 3))
