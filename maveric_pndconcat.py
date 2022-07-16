import pandas as pd
df = pd.read_csv("/Users/purnaraghavaraokalva/Documents/GitHub/test2/insurance.csv")
df1 = pd.DataFrame({
"age":[4, 5, 6, 7],
"sex":["female", "male", "male","male"],
"bmi":[27, 31, 42, 46],
"children":[0, 5, 4, 7],
"smoker":["yes", "no", "yes", "no"],
"region":["northeast", "northwest", "north", "south"],
"charges":[4556, 4550, 7888, 5666]
})
#print(df1)
#print(df1.append(df))
#df2 = pd.concat(df1, df, axis=0)
print(df2)
f1 = df.append(df1, ignore_index=True)

print(f1)
print(f1.shape())
