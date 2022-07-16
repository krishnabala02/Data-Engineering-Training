from zipfile import ZipFile

filename = []
filesize = []
# Create a ZipFile Object and load sample.zip in it
Path = '/Users/purnaraghavaraokalva/Desktop/sampledata.zip'
with ZipFile(Path, 'r') as zipObj:
    # Get list of files names in zip
    listOffiles = zipObj.namelist()
    #sizeoffiles = zipObj.file_size
    listoffilesinsidezip = []
    print(type(listOffiles))
    # Iterate over the list of file names in given list & print them
# file_end = ".txt"
for key, filename in enumerate(listOffiles):
    if filename.__contains__("_"):
        continue
    if filename.__contains__(".txt"):
       listoffilesinsidezip.append(filename.split("/")[1])
       print(listoffilesinsidezip)
