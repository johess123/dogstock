# line notify
import requests,json
import time

def lineNotifyMessage(token, msg):
    headers = {
        "Authorization": "Bearer " + token, 
        "Content-Type" : "application/x-www-form-urlencoded"
    }
    payload = {'message': msg }
    r = requests.post("https://notify-api.line.me/api/notify", headers = headers, params = payload)
    return r.status_code
def now():
    url="https://www.taiwanindex.com.tw/site/indexDBJSON"
    try: # 連線
        res = requests.get(url)
    except:
        print("connection error")
        return 0
    resjson = json.loads(res.text) # 讀檔
    date = resjson['msgArray'][1]['t'] # 日期
    price = resjson['msgArray'][1]['z'] # 收盤價
    date1 = ""
    for i in range(10):
        if i != 4 and i != 7:
            date1 += date[i]
    token = '2J07SccHN631cmTolFNcSZH2Rg3B08ClsL5tKrD2Pf8'
    message = '日期：'+str(date)+" 收盤價："+str(price)
    lineNotifyMessage(token, message)
    # try:
    #     #sql = "INSERT INTO analysis_cp(date,price) values (%s,%s)"
    #     #cur.execute(sql,(date,price))
    #     #conn.commit()
    #     print("Insert successfully")
    #     return 1
    # except:
    #     print("You have inserted CP of today")
    #     return 2

def main():
  #token = '2J07SccHN631cmTolFNcSZH2Rg3B08ClsL5tKrD2Pf8'
  #message = '基本功能測試'
  #lineNotifyMessage(token, message)
    while True:
        do = True
        while do:
            now()
            time.sleep(10)
main()