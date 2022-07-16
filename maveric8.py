
list = [1, 2, 3, 4]
a = [i for i in list if i % 2 == 0]
print(a)
b = [i for i in list if i % 2 != 0]
print(b)
z = lambda i: i ** 2
x = [z(i) for i in list if i % 2 == 0]
print(x)
y = [z(i) for i in list]
print(y)
