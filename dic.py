test_dict = {'gfg': [5, 6, 7, 8],
             'is': [10, 11, 7, 5],
             'best': [6, 12, 10, 8],
             'for': [1, 2, 5]}
test_dict1 = []
x = len(test_dict)

print(test_dict['is'])
for i, value in enumerate(test_dict):
   test_dict1.extend(test_dict[value])
     print(test_dict1)




