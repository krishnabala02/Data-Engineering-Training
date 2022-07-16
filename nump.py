import numpy as np
a =np.array([[ 1 , 2 , 3 , 4],[ 5 , 6 , 7 , 8],[ 9 ,10 ,11,12]])
col_r1 =np.array(a[:,1]).flatten()
col_r2 =np.array(a[:,1])
arr_2d=np.reshape(col_r2, (1,3))
print(arr_2d)
print(col_r1)



