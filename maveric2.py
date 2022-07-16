a = [2, 4, 7, 8, 9, 10]
even = list(filter(lambda x: x % 2 == 0, a))
print(even)
z = map(lambda x: x ** 2, even)
print(list(z))


def sum(a, b):
    return a + b
