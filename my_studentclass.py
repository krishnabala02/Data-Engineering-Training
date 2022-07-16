class Student:
    # A simple class
    # attribute
    attr1 = "first year"
    attr2 = "4 year course"

    # A sample method
    def desc(self):
        print("I'm in", self.attr1)
        print("I have enrolled to", self.attr2)


# Driver code
# Object instantiation
Riyan = Student()

# Accessing class attributes
# and method through objects
print(Riyan.attr1)
Riyan.desc()