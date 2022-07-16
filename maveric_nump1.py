import pandas as pd
import numpy as np
from numpy import dtype

arr =np.array([[784,  78569],
       [850, 568956],
       [400,     56]])
df = pd.DataFrame([[784,  78569],
       [850, 568956],
       [400,     56]])
arr2 =np.array([[784,  78569, 700,800],
       [850, 568956, 89, 90],
       [400,56, 89, 976]])
arr3 =np.array([[[1,2,3], [3,4,5]],
                [[2,4,5], [5,6,7]]])
#print(arr3.ndim)
#print(arr2.ndim)
#print(df)
#df1 = df.sum(axis=1)
#df2 =df.sum(axis=0)
#df3 =df.drop(0, axis="columns")
#df4 = df.drop(0, axis="rows")
#print(arr.reshape(2,3))
#df5 = np.resize(arr, (3,3))
#arr[0,3]= 4
#print(arr.flatten())
#print(arr.tolist())
#print(np.random.randint(45, size=(4,4)))
#print(np.random.rand(5))
#print(np.random.rand(4, 4))
#print(np.ones((2, 2)))
#print(np.zeros((4, 4)))
#print(arr[:, 0])
#print(arr2[:, 1:3])
#print(arr3[:,1:2,1])
#np.linspace(0,50,7)
#np.arange(0, 20, 2)
#np.eye(5)
#arr[:, 0] =np.add(arr[:, 0],5)
#arr[:, 0] =np.divide(arr[:, 0],2)
#print(arr[:, 0])
#arr.sort(axis = 0)
#arr4 =arr.copy()
#print(arr4)
#print(np.delete(arr4, 1, axis=0))
#print(np.delete(arr4, 1, axis=1))
#print(np.delete(arr4, 1, axis=1))
#print(np.delete(arr4, 0, axis=0))
#print(np.delete(arr4, 0, axis=1))
#print(np.array(pow(arr, 3)))
arr5 = np.array([[1.1, 2, 3], [2.3, 0, 4.5]])
#print(np.add(arr5, 1.))






