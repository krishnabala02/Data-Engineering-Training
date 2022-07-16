import pandas as pd
import numpy as np
data = {'animal': ['cat', 'cat', 'snake', 'dog', 'dog', 'cat', 'snake', 'cat', 'dog', 'dog'],
        'age': [2.5, 3, 0.5, np.nan, 5, 2, 4.5, np.nan, 7, 3],
        'visits': [1, 3, 2, 3, 2, 3, 1, 1, 2, 1],
        'priority': ['yes', 'yes', 'no', 'yes', 'no', 'no', 'no', 'yes', 'no', 'no']}

labels = ['a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j']

df = pd.DataFrame(data, index=['a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j'])
#print(df['visits'].sum())
#print(df)
#print(df[["animal", "age"]])
#print(df.where(df["visits"] > 3))
#print(df.groupby('animal').size())
#print(df['animal'].value_counts())
#df = df.replace({'priority': {'yes': True, 'no': False}})
#print(df)
#print(df.pivot_table(index='animal', columns='visits', values='age', aggfunc=np.mean))
#print(df.drop_duplicates(subset=None, keep='first', inplace=False))
#df1 = pd.DataFrame(np.random.random(size=(5, 3)))
#print(df1)
#print(df1.mean(axis=1))
#print(df1.sub(df1.mean(axis=1), axis=0))
#df2 = pd.DataFrame(np.random.random(size=(5, 10)),columns=list('abcdefghij'))
#print(df2)
#print(df2.sum().idxmin())
#df3 = pd.DataFrame(np.random.randint(0, 2, size=(10, 3)))
#df3 = pd.DataFrame(np.random.randint(0, 2, size=(10, 3)))
#print(df3)
#print(df3.groupby(df3.columns.tolist(), as_index=False).size())

#print(len(df3.drop_duplicates(keep=False)))


#df4 = pd.DataFrame({'grps': list('aaabbcaabcccbbc'), 'vals': [12,345,3,1,45,14,4,52,54,23,235,21,57,3,87]})
#print(df4)
#print(df4.groupby('grps')['vals'].nlargest(3))
#print(df4.groupby('grps')['vals'].nlargest(3).sum(level="grps"))

#df5 = pd.DataFrame(np.random.RandomState(8765).randint(1, 101, size=(100, 2)), columns = ["A", "B"])
#df5 = pd.DataFrame(np.random.RandomState(8765).randint(1, 101, size=(100, 2)), columns = ["A", "B"])
#print(pd.cut(np.array([1, 2, 3, 4, 5, 6, 7, 8]), np.arange(0, 8, 2)))
#print(pd.cut(df5['A'], np.arange(0, 101, 10)))
#print(df5.groupby(pd.cut(df5['A'], np.arange(0, 101, 10)))['B'].sum())
df = pd.DataFrame({'From_To': ['LoNDon_paris', 'MAdrid_miLAN', 'londON_StockhOlm',
                               'Budapest_PaRis', 'Brussels_londOn'],
                'FlightNumber': [10045, np.nan, 10065, np.nan, 10085],
                'RecentDelays': [[23, 47], [], [24, 43, 87], [13], [67, 32]],
                     'Airline': ['KLM(!)', '<Air France> (12)', '(British Airways. )',
                                   '12. Air France', '"Swiss Air"']})
#DataFrame.interpolate(method=’linear’, axis=0, limit=None, inplace=False, limit_direction=’forward’, limit_area=None, downcast=None, **kwargs)
#df['FlightNumber'] = df['FlightNumber'].interpolate(method="linear").astype(int)
# print(df)
#print(df)
#temp = df.From_To.str.split('_', expand=True)
#print(df.From_To.str.split('_', expand=True))
#temp.columns = ['From', 'To']
#print(temp)
df1 = pd.DataFrame({'From_To': ['LoNDon_paris', 'MAdrid_miLAN', 'londON_StockhOlm',
                                 'Budapest_PaRis', 'Brussels_londOn'],
                'FlightNumber': [10045, np.nan, 10065, np.nan, 10085],
                'RecentDelays': [[23, 47], [], [24, 43, 87], [13], [67, 32]],
                     'Airline': ['KLM(!)', '<Air France> (12)', '(British Airways. )',
                                 '12. Air France', '"Swiss Air"']})

df = pd.DataFrame([pd.Series(x) for x in df1.RecentDelays])
print(df)
df.columns = ['team_{}'.format(x+1) for x in df.columns]
df3 = df1.drop('RecentDelays', axis=1).join(df)
print(df3)






