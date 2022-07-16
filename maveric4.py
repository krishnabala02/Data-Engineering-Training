list = [1, 2, 3, "data", 6, -8, 10.0]
out =[]
while list:
    mv = list[0]
    print(type(mv))
    for i in list:
        if str(i).isalpha():
            list.remove(i)
        else:
            if i < mv:
                mv = i
                out.append(mv)
            else:
                continue
        list.remove(i)

print(out)



