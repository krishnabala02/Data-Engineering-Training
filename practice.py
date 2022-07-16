import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
df=pd.read_csv("/Users/purnaraghavaraokalva/Desktop/csvfiles/insurance.csv")
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

# x axis values
x = [1,2,3]
# corresponding y axis values
y = [2,4,1]

# plotting the points
plt.plot(x, y)

# naming the x axis
plt.xlabel('x - axis')
# naming the y axis
plt.ylabel('y - axis')

# giving a title to my graph
plt.title('My first graph!')

# function to show the plot
plt.show()



















