mylist = [1, 2, 3, 4, 5]
a = len(mylist) - 1
temp = 1
for i in mylist:
 temp = mylist[0]
mylist[0] = mylist[a]
mylist[a] = temp

print(mylist)


