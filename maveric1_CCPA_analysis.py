import pandas as pd
df = pd.read_csv("/Users/purnaraghavaraokalva/Desktop/ccpp/combined_cycle_power_plant.csv",delimiter=';')
df2= df.drop_duplicates()
print(df.size)
print(df2.size)
'''
47840
47635
'''