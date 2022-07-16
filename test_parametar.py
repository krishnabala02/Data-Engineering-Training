import math_file
import pytest


@pytest.mark.parametrize(a, b, result, [(5, 6, 11), (2, 3, 5)])
def add(a, b):
    assert math_file.add(a, b) = result
