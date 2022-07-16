import area_rectangle


def test_area():
    out = area_rectangle.area(2, 3)
    assert out == 6


def test_perimeter():
    out = area_rectangle.perimeter(5, 3)
    assert out == 16
