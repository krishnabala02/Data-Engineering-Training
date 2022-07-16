def outer_function(fun):
    def inner_function(a, b):
        if a > b:
            a = a + 3
            b = b + 4
        return fun(a, b)

    return inner_function


@outer_function
def add(a, b):
    return a + b


x = add(5, 3)
print(x)
