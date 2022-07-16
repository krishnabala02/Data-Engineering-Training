f = open("demofile.txt", "r")
f.write("my first file handling")
f.close()

import requests as requests

server_text = requests.get('https://www.google.com/').text
f = open(r'response.html', 'w')
f.write(server_text)
stripped = requests.sub('<[^<]+?>', '', content)
print(stripped)