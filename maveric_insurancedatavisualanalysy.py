import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

df = pd.read_csv("/Users/purnaraghavaraokalva/Desktop/csvfiles/insurance.csv")
sns.set()
# plt.figure(figsize = (6,6), dpi = 400)
# sns.barplot( x = 'children', y = 'charges', data = df)
# sns.barplot( x = 'sex', y = 'charges', data = df)
# sns.barplot( x = 'smoker', y = 'charges', data = df)
#sns.scatterplot(x='age', y='charges', data=df)
#sns.scatterplot(x='bmi', y='charges', data=df)
#sns.barplot(x='region', y='charges', data=df)
#g=sns.PairGrid(df,y_vars="charges",x_vars=["age","sex","smoker"])
#g.map(sns.pointplot,scale=1.3)
#g.set(ylim=(0,63770))
#sns.barplot(x='children', y='charges', data=df)
sns.boxplot(x='children', y='charges', data=df)
#sns.scatterplot(x='region', y='charges', data=df)
plt.show()
#sns.despine(fig=g)
