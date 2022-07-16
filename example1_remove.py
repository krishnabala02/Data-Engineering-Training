import xml.etree.ElementTree as ET
mytree = ET.parse('sample2.xml')
myroot = mytree.getroot()
foods = myroot.findall('food')
for key,value in enumerate(foods):
    myroot[key].remove(myroot[key][1])
    mytree.write('example1_remove.xml')