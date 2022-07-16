list = [4, 1, 7, "data", 2, 5.6]
print(type(list[0]))
list1 = []
for s in list:
    if isinstance(s, int) or isinstance(s, float):
        list1.append(s)

print(list1)
# print(list1.sort())
def bubbleSort(arr):
    n = len(arr)
    print(f"arr {arr}")
    swapped = False
    for i in range(n - 1):
        print(i)
        for j in range(0, n - i - 1):
            print(j)
            if arr[j] > arr[j + 1]:
                swapped = True
                if arr[j+1] == 5.6:
                    print(type(arr[j+1]))
                arr[j], arr[j + 1] = float(arr[j + 1]) if isinstance(arr[j+1], float) else arr[j + 1] , float(arr[j]) if isinstance(arr[j], float) else arr[j]
        if not swapped:
            return

bubbleSort(list1)
print("Sorted list of numbers is:")
print(list1)
for i in range(len(list1)):
    print("% d" % list1[i], end=" ")


