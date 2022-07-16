import pandas as pd

my_dataset = {"cars": ["bmw", "honda", "benz"], "passings": ["1", "2", "3"]}
my_dataset1 = ["1", "2", "3"]
df1 = pd.Series(my_dataset1, index=["x", "y", "z"])
df = pd.DataFrame(my_dataset)
print(df1)
print(df.loc[[0, 1]])
