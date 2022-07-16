def upper_case(func):
    def inner_upper_case(text):
        data = text.upper()
        return upper_case(data)

    inner_upper_case()