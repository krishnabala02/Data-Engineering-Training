# generator

def square(n):
    for i in range(n):
        yield i ** 2


out = square(5)
for i in out:
    print(i)
#next(out)

print(type(out))
