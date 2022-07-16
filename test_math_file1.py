import math_file
import pytest


# pytest test_mathfile.py
# pytest test_mathfile.py
# pytest -v
# pytest -v test_mathfile.py
# pytest test_mathfile.py::test_add
# pytest test_mathfile.py::test_add -v
# pytest test_mathfile.py -v -k "add"


@pytest.mark.number
def test_add():
    out = math_file.add(2, 3)
    assert out == 5
    assert math_file.mul(15, 10) == 25


@pytest.mark.character
def test_add():
    assert math_file.add("heloo", "testing") == "helootesting"
