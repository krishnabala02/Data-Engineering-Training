import math_file


# pytest test_mathfile.py
# pytest test_mathfile.py
# pytest -v
# pytest -v test_mathfile.py
# pytest test_mathfile.py::test_add
# pytest test_mathfile.py::test_add -v
# pytest test_mathfile.py -v -k "add"



def test_add():
    out = math_file.add(2, 3)
    assert out == 5


def test_sub():
    out = math_file.sub(5, 3)
    assert out == 2


def test_mul():
    out = math_file.mul(5, 3)
    out1 = math_file.mul(15, 10)
    assert out == 15
    assert out1 == 15
