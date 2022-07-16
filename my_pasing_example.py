import xml.etree.ElementTree as ET
import pandas as pd
mytree = ET.parse('sample2.xml')
myroot = mytree.getroot()
foods = myroot.findall('food')

data = []
cols = []
for i, child in enumerate(foods):
    data.append([subchild.text for subchild in child])
    cols.append(foods[i].tag)

df = pd.DataFrame(data).T  # Write in DF and transpose it
df.columns = cols  # Update column names
print(df)


