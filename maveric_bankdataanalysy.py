import pandas as pd
df = pd.read_csv("Bank_churn_modelling - Copy.csv")
df1 = df.query('Exited== 1')
g = df.groupby(["Exited"])
#for exited, dataframe in g:
    #print(exited)
    #print(dataframe)

exited_bank = g.get_group(1)
present_bank = g.get_group(0)
#print(exited_bank.count())

h = df1.groupby(["Gender"])#["Votes"].sum().sort_values(ascending=False).head(1)
#for gender, dataframe in h:
    #print(gender)
    #print(dataframe)

female = h.get_group("female")
male = h.get_group("male")

print(female)



