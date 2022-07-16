import pandas as pd
df=pd.read_csv("/Users/purnaraghavaraokalva/Desktop/csvfiles/insurance.csv")
print(df)
df2 = df.drop_duplicates()
print(df.shape)
print(df2.shape)
'''
[1338 rows x 7 columns]
(1338, 7)
(1337, 7)

'''
print(df)
print(df.isna().any())
'''
[1338 rows x 7 columns]
age         False
sex         False
bmi         False
children    False
smoker      False
region      False
charges     False
dtype: bool
'''
print(df.info())
'''
 #   Column    Non-Null Count  Dtype  
---  ------    --------------  -----  
 0   age       1338 non-null   int64  
 1   sex       1338 non-null   object 
 2   bmi       1338 non-null   float64
 3   children  1338 non-null   int64  
 4   smoker    1338 non-null   object 
 5   region    1338 non-null   object 
 6   charges   1338 non-null   float64
dtypes: float64(2), int64(2), object(3)
memory usage: 73.3+ KB
'''
print(df.describe())
'''
               age          bmi     children       charges
count  1338.000000  1338.000000  1338.000000   1338.000000
mean     39.207025    30.663397     1.094918  13270.422265
std      14.049960     6.098187     1.205493  12110.011237
min      18.000000    15.960000     0.000000   1121.873900
25%      27.000000    26.296250     0.000000   4740.287150
50%      39.000000    30.400000     1.000000   9382.033000
75%      51.000000    34.693750     2.000000  16639.912515
max      64.000000    53.130000     5.000000  63770.428010

'''
print(df["sex"].unique())
print(df["sex"].nunique())
print(df["smoker"].unique())
print(df["smoker"].nunique())
print(df["region"].unique())
print(df["region"].nunique())
'''
['female' 'male']
2
['yes' 'no']
2
['southwest' 'southeast' 'northwest' 'northeast']
4
'''
#By looking at this we can say that older people have to pay more
print(df[["age","charges"]].corr())
'''
              age   charges
age      1.000000  0.299008
charges  0.299008  1.000000
'''
total_count=df["charges"].count()
print(df.groupby("age").count())
f = df.groupby("age")
df["charges_18_30"]=df["charges"].where(df["age"].between(18,30))
print("max and min charges collected from ages between 18 and 30")
print(df["charges_18_30"].max())
print(df["charges_18_30"].min())
df["charges_30_45"]=df["charges"].where(df["age"].between(30,45))
print("max and min charges collected from ages between 30 and 45")
print(df["charges_30_45"].max())
print(df["charges_30_45"].min())
df["charges_45_64"]=df["charges"].where(df["age"].between(45,64))
print("max and min charges collected from ages between 45 and 64")
print(df["charges_45_64"].max())
print(df["charges_45_64"].min())
'''
max and min charges collected from ages between 18 and 30
51194.55914
1121.8739
max and min charges collected from ages between 30 and 45
62592.87309
3260.199
max and min charges collected from ages between 45 and 64
63770.42801
7147.105
'''
#By looking at this we can say that people having high BMI to pay more
print(df[["bmi","charges"]].corr())
'''
              bmi   charges
bmi      1.000000  0.198341
charges  0.198341  1.000000
'''
#By looking at this we can say that having more children reduces insurance charges
print(df[["children","charges"]].corr())

'''
   children   charges
children  1.000000  0.067998
charges   0.067998  1.000000
'''
import pandas as pd
df=pd.read_csv("/Users/purnaraghavaraokalva/Desktop/csvfiles/insurance.csv")
#count= df.group_by("smoker").values_count()
#print(count)
#print(df.groupby("sex").count())
total_count=df["charges"].count()
print(df.groupby("children").count())
f = df.groupby("children")
children_0=f.get_group(0)
children_1=f.get_group(1)
children_2=f.get_group(2)
children_3=f.get_group(3)
children_4=f.get_group(4)
children_5=f.get_group(5)
print("maximum and minimum charges paid for having 0 children")
print(children_0["charges"].max())
print(children_0["charges"].min())
print("maximum and minimum charges paid for having 1 children")
print(children_1["charges"].max())
print(children_1["charges"].min())
print("maximum and minimum charges paid for having 2 children")
print(children_2["charges"].max())
print(children_2["charges"].min())
print("maximum and minimum charges paid for having 3 children")
print(children_3["charges"].max())
print(children_3["charges"].min())
print("maximum and minimum charges paid for having 4 children")
print(children_4["charges"].max())
print(children_4["charges"].min())
print("maximum and minimum charges paid for having 5 children")
print(children_5["charges"].max())
print(children_5["charges"].min())

'''
age  sex  bmi  smoker  region  charges
children                                        
0         574  574  574     574     574      574
1         324  324  324     324     324      324
2         240  240  240     240     240      240
3         157  157  157     157     157      157
4          25   25   25      25      25       25
5          18   18   18      18      18       18
maximum and minimum charges paid for having 0 children
63770.42801
1121.8739
maximum and minimum charges paid for having 1 children
58571.07448
1711.0268
maximum and minimum charges paid for having 2 children
49577.6624
2304.0022
maximum and minimum charges paid for having 3 children
60021.39897
3443.064
maximum and minimum charges paid for having 4 children
40182.246
4504.6624
maximum and minimum charges paid for having 5 children
19023.26
4687.797

'''

print(df.groupby("region")["charges"].max())
'''
region
northeast    58571.07448
northwest    60021.39897
southeast    63770.42801
southwest    52590.82939

'''

print(df.groupby("sex")["charges"].max())
'''
sex
female    63770.42801
male      62592.87309

'''
#By the below analysys we can say smoker has to pay more than nonsmoker

total_count=df["charges"].count()
print(df.groupby("smoker").count())
f = df.groupby("smoker")
smoker=f.get_group("yes")
nonsmoker = f.get_group("no")
smoker_count=smoker["charges"].count()
nonsmoker_count=nonsmoker["charges"].count()
percent_smoker= (smoker_count/total_count)*100
percent_nonsmoker= (nonsmoker_count/total_count)*100
print(percent_smoker)
print(percent_nonsmoker)
'''
        age   sex   bmi  children  region  charges
smoker                                             
no      1064  1064  1064      1064    1064     1064
yes      274   274   274       274     274      274
20.47832585949178
79.52167414050822
'''
print(df["charges"].where(df["smoker"]=="yes").max())
print(df["charges"].where(df["smoker"]=="yes").min())
print(df["charges"].where(df["smoker"]=="no").max())
print(df["charges"].where(df["smoker"]=="no").min())
'''
63770.42801 smoker maximum value
12829.4551 smoker minimum value
36910.60803 non smoker maximum value
1121.8739 non smoker minimum value
'''
#So on charges sex has no effect

total_count = df["charges"].count()
print(df.groupby("sex").count())
f = df.groupby("sex")
female = f.get_group("female")
male = f.get_group("male")
female_count = female["charges"].count()
male_count = male["charges"].count()
percent_female = (female_count / total_count) * 100
percent_male = (female_count / total_count) * 100
print(percent_female)
print(percent_male)
'''
  age  bmi  children  smoker  region  charges
sex                                                
female  662  662       662     662     662      662
male    676  676       676     676     676      676
49.47683109118087
49.47683109118087
'''

print(df["charges"].where(df["sex"]=="female").max())
print(df["charges"].where(df["sex"]=="female").min())
print(df["charges"].where(df["sex"]=="male").max())
print(df["charges"].where(df["sex"]=="male").min())
'''
63770.42801 female maximum value
1607.5101 female minimum value
62592.87309 male maximum value
1121.8739 male miniimum value
'''
# Region has no effect on charges paid
total_count = df["charges"].count()
print(df.groupby("region").count())
f = df.groupby("region")
southwest = f.get_group("southwest")
southeast = f.get_group("southeast")
northwest = f.get_group("northwest")
northeast = f.get_group("northeast")
southwest_count = southwest["charges"].count()
southeast_count = southeast["charges"].count()
northwest_count = northwest["charges"].count()
northeast_count = northeast["charges"].count()

percent_southwest = (southwest_count / total_count) * 100
percent_southeast = (southeast_count / total_count) * 100
percent_northwest = (northwest_count / total_count) * 100
percent_northeast = (northeast_count / total_count) * 100
print(percent_southwest)
print(percent_southeast)
print(percent_northwest)
print(percent_northeast)
'''
       age  sex  bmi  children  smoker  charges
region                                             
northeast  324  324  324       324     324      324
northwest  325  325  325       325     325      325
southeast  364  364  364       364     364      364
southwest  325  325  325       325     325      325
24.28998505231689
27.204783258594915
24.28998505231689
24.2152466367713
'''
print(df["charges"].where(df["region"]=="southwest").max())
print(df["charges"].where(df["region"]=="southeast").max())
print(df["charges"].where(df["region"]=="northwest").max())
print(df["charges"].where(df["region"]=="northeast").max())
print(df["charges"].where(df["region"]=="southwest").min())
print(df["charges"].where(df["region"]=="southeast").min())
print(df["charges"].where(df["region"]=="northwest").min())
print(df["charges"].where(df["region"]=="northeast").min())
'''
52590.82939
63770.42801
60021.39897
58571.07448
1241.565
1121.8739
1621.3402
1694.7964
'''
'''
#OVER ALL FINDINGS:
1.older people have to pay more
2.people having high BMI to pay more
3. more children reduces insurance charges
4.smoker has to pay more than nonsmoker
5.Region has no effect on charges paid
6.sex has no effect

'''
