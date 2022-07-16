import pandas as pd
import numpy as np

df = pd.read_csv("insurance.csv")
g = df.groupby("sex")
female = g.get_group("female")
male = g.get_group("male")
#print(female.sort_values(by=["bmi"]).head(50))
#print(male.sort_values(by=["bmi"]).head(50))

#out = pd.crosstab(df["age"], df["children"])
out1 = pd.crosstab([df["age"], df["children"]], df["region"], margins=True)
# out2 = pd.crosstab(df["sex"], [df["age"], df["children"]], margins= True)
# out3 = pd.crosstab(df["region"], df["charges"], margins= True)
# print(out3)
# print(out1)
# print(out2)
#out = pd.crosstab(df["sex"], df["smoker"], values=df["charges"], aggfunc=np.average, margins=True)
print(out1)
# print(out['yes']/out['All'])

# print(type(g))
#for gender, dataframe in g:
    #print(gender)
    #print(dataframe)
#g = df.groupby("sex")
#female = g.get_group("female")
#male = g.get_group("male")
#print(female.sort_values(by=["bmi"]).head(50))
#print(female)

#print(female.set_option("display_maximum_rows", 50))
#print(male.set_option("display_maximum_rows", 50))

#print(female.sort_values(by=["bmi"]))
#print(male.count())
