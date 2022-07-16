import pandas as pd
df = pd.read_csv("/Users/purnaraghavaraokalva/Desktop/ccpp/combined_cycle_power_plant.csv",delimiter=';')
df2 = df.drop_duplicates()
print(df.shape)
print(df2.shape)
'''
(9568, 5)
(9527, 5)
'''
print(df)
print(df.isna().any())
'''
temperature          False
exhaust_vacuum       False
ambient_pressure     False
relative_humidity    False
energy_output        False
#Here there is no missing data so no need to clean data
'''
print(df.info())
'''
#   Column             Non-Null Count  Dtype  
---  ------             --------------  -----  
 0   temperature        9568 non-null   float64
 1   exhaust_vacuum     9568 non-null   float64
 2   ambient_pressure   9568 non-null   float64
 3   relative_humidity  9568 non-null   float64
 4   energy_output      9568 non-null   float64
dtypes: float64(5)
'''

print(df.describe())
'''
        temperature  exhaust_vacuum  ...  relative_humidity  energy_output
count  9568.000000     9568.000000  ...        9568.000000    9568.000000
mean     19.651231       54.305804  ...          73.308978     454.365009
std       7.452473       12.707893  ...          14.600269      17.066995
min       1.810000       25.360000  ...          25.560000     420.260000
25%      13.510000       41.740000  ...          63.327500     439.750000
50%      20.345000       52.080000  ...          74.975000     451.550000
75%      25.720000       66.540000  ...          84.830000     468.430000
max      37.110000       81.560000  ...         100.160000     495.760000
'''
#gas energy output depends on ambient variable (exhaust_vaccum) which effects steam turbine generation so finding relation between them
#here here -0.86978 indicates  good relation between EV AND energy output but if we increase one the other will go down
print(df[["exhaust_vacuum","energy_output"]].corr())
'''
               exhaust_vacuum  energy_output
exhaust_vacuum         1.00000       -0.86978
energy_output         -0.86978        1.00000
'''

# As gas turbine power generation depends on these 3 environmental factors to know how these three are interrelated as it effects output power
#here temperature has good relation with ambient pressure and relative humidity but if temperature increase both will decrease
#ambient_pressure does not have good relation with relative humidity
print(df[["temperature","ambient_pressure","relative_humidity"]].corr())
'''
                     temperature  ambient_pressure  relative_humidity
temperature           1.000000         -0.507549          -0.542535
ambient_pressure     -0.507549          1.000000           0.099574
relative_humidity    -0.542535          0.099574           1.000000
'''
#As these ambient variables effect generation from gas turbines
#temperature and energy output has good relation but if one increases other decrease
#so high temperatures reduce output power generation from gas turbine
#ambient pressure and output power has good relation
#relative humidity doenot have good relation with energy output it effects output power
print(df[["temperature","energy_output"]].corr())
print(df[["ambient_pressure","energy_output"]].corr())
print(df[["relative_humidity","energy_output"]].corr())
'''
                temperature  energy_output
temperature       1.000000      -0.948128
energy_output    -0.948128       1.000000
                  ambient_pressure  energy_output
ambient_pressure          1.000000       0.518429
energy_output             0.518429       1.000000
                   relative_humidity  energy_output
relative_humidity           1.000000       0.389794
energy_output               0.389794       1.000000

'''


#Effect of environment variables on output power
print(df[["temperature","ambient_pressure","relative_humidity","exhaust_vacuum"]].corr())
print(df[["temperature","ambient_pressure","relative_humidity","exhaust_vacuum","energy_output"]].corr())
'''
                    temperature  ...  exhaust_vacuum
temperature           1.000000  ...        0.844107
ambient_pressure     -0.507549  ...       -0.413502
relative_humidity    -0.542535  ...       -0.312187
exhaust_vacuum        0.844107  ...        1.000000

[4 rows x 4 columns]
                   temperature  ambient_pressure  ...  exhaust_vacuum  energy_output
temperature           1.000000         -0.507549  ...        0.844107      -0.948128
ambient_pressure     -0.507549          1.000000  ...       -0.413502       0.518429
relative_humidity    -0.542535          0.099574  ...       -0.312187       0.389794
exhaust_vacuum        0.844107         -0.413502  ...        1.000000      -0.869780
energy_output        -0.948128          0.518429  ...       -0.869780       1.000000'
'''
#what environmental variables combination yields peak output
print(max(df["energy_output"].tolist()))
peak_output=max(df["energy_output"].tolist())
print(df.loc[df["energy_output"] == peak_output])
'''
           temperature  exhaust_vacuum  ...  relative_humidity  energy_output
4487         5.48           40.07  ...              65.62         495.76
'''