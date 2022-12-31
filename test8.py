# 傳送提醒訊息
import os, sys
import time
import requests

token = sys.argv[1]
msg = sys.argv[2]+" "+sys.argv[3]+" "+sys.argv[4]
times = int(sys.argv[5])

headers = {
    "Authorization": "Bearer " + token, 
    "Content-Type" : "application/x-www-form-urlencoded"
}
payload = {'message': msg }
r = requests.post("https://notify-api.line.me/api/notify", headers = headers, params = payload)

for i in range(times-1):
    time.sleep(10)
    headers = {
        "Authorization": "Bearer " + token, 
        "Content-Type" : "application/x-www-form-urlencoded"
    }
    payload = {'message': msg }
    r = requests.post("https://notify-api.line.me/api/notify", headers = headers, params = payload)

os.system("taskkill /f /im test7.exe")