a = (1, 2, 3, 4)


def sqr(n):
    return n ** 2


b = map(sqr, a)
print(list(b))
