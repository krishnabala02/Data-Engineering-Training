import os
f = open('my_text.txt', 'a')
f.write("Now the file has more content!")
f.close()
f = open('my_text.txt', 'r')
print(f.read())
f = open("myfile1.txt", "x")
#f = open("myfile2.txt", "w")
os.remove("example1_update.xml")
