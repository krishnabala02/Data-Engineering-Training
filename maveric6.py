list = [4, 1, 7, "data", 2, 5.6]
print(type(list[0]))
list1 = []
for s in list:
    if isinstance(s, int) or isinstance(s, float):
        list1.append(s)
print(list1)
def bubbleSort(lis):
    n = len(lis)
    swapped = False
    for i in range(n - 1):
        for j in range(0, n - i - 1):
            if lis[j] > lis[j + 1]:
                swapped = True
                lis[j], lis[j + 1] = lis[j + 1], lis[j]
        if not swapped:
            return
bubbleSort(list1)
print(list1)
