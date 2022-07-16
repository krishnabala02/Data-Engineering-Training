class Vehicle:
    vehicle1 = "Car"
    vehicle2 = "Bike"
    vechile3 = "cycle"

    def __init__(self, name):
        self.name = name

    def maxspeed(self, speed):
        self.speed = speed
        print(self.name + "Maximum speed is " + self.speed)

C1 =Vehicle('BMW')
B1 = Vehicle('HONDA')
S1 = Vehicle('LADY BIRD')
print(C1.maxspeed("300"))
print(B1.maxspeed("100"))
print(S1.maxspeed("50"))


