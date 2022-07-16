import requests as requests
response_text = requests.get("https://www.google.com/").text
f = open('response.html', 'w')
f.write(response_text)



import requests
url = 'https://google.com/'
result=""
try:
    is_up = requests.get(url).status_code==200
    if is_up== False:
        raise Exception(is_up)
except:
    result='Failed'
else:
    result='<meta http-equiv = "refresh" content = "3; url = https://www.google.com" />'
finally:
    f= open(r'result.html','w')
    f.write(result)
    f.close()