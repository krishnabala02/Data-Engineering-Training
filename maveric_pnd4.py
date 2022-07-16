import pandas as pd


df = pd.read_csv("insurance.csv")

df1 = df.describe()
df1.to_excel("Bank_churn_analysis.xlsx")
