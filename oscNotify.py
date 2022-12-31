# 提醒跌過OSC
import mysql.connector
from bs4 import BeautifulSoup
import requests
import pandas as pd
import talib
import datetime
import time
import win32api

def db():
    try:
        conn = mysql.connector.connect(
            user="root",
            password="",
            host="127.0.0.1",
            port=3307,
            database="stock"
        )
    except:
        print("connection db error")
        exit(1)
    cur=conn.cursor()
    return conn,cur

def db1():
    try:
        conn1 = mysql.connector.connect(
            user="kenny",
            password="Kenny061256",
            host="122.254.33.183",
            port=1440,
            database="stock"
        )
    except:
        print("connection db error1")
        exit(1)
    cur1=conn1.cursor()
    return conn1,cur1

def countOsc(stockid,allNotify,allPrice,no):
    if done[no] == True:
        # 計算OSC
        df = pd.DataFrame(allPrice,columns = ["Close"])
        dif,macd,osc = talib.MACD(df['Close'],12,26,9)
        # 如果今日OSC小於昨日OSC，發出提醒
        osc = osc.tolist()
        if osc[-1] < osc[-2]:
            done[no] = False
            # 提醒跌過OSC
            for i in range(len(allNotify)):
                stockname = allNotify[i][1]
                token = allNotify[i][2]
                times = allNotify[i][3]
                message = '使用者您好，'+str(stockid)+" "+stockname+" OSC小於昨日！"
                print(message)
                win32api.ShellExecute(0, "open", "test8.py", f"{token} {message} {times}", "", 0)
            message = '使用者您好，'+str(stockid)+" "+stockname+" OSC小於昨日！"
            print(message)

def startSearch(stockid,allNotify,allPrice):
    # 爬取及時股價
    do = True
    while do == True:
        try:
            nowPrice = [[] for i in range(len(stockid))]
            ##
            print("開始抓取資料")
            url = "https://histock.tw/stock/rank.aspx?p=all"
            res = requests.get(url)
            soup = BeautifulSoup(res.text,'html.parser')
            stock = soup.find('table',id='CPHB1_gv')
            allStock = stock.getText().split("\n")
            i = 4
            myStock = []
            while i < len(allStock)-1:
                myStock1 = []
                for j in range(i,i+16):
                    myStock1.append(allStock[j])
                myStock.append(myStock1)
                i += 16
            idAndPrice = []
            for i in range(len(myStock)):
                idAndPrice.append([myStock[i][0],myStock[i][3]])
            idAndPrice = sorted(idAndPrice)
            for i in range(len(idAndPrice)):
                if idAndPrice[i][0] in stockid:
                    nowPrice[stockid.index(idAndPrice[i][0])] = float(idAndPrice[i][1])
            for i in range(len(stockid)):
                if nowPrice[i] == []:
                    nowPrice[i] = False
            if len(nowPrice) == len(stockid):
                do = False
            else:
               print("資料未抓齊")
               time.sleep(1)
        except:
           print("連線失敗")
           time.sleep(1)
    # 把即時股價也加入歷史股價中
    for i in range(len(nowPrice)):
        allPrice[i].append(nowPrice[i])
    # 計算OSC
    for i in range(len(stockid)):
        if nowPrice[i] != False:
            countOsc(stockid[i],allNotify[i],allPrice[i],i)

def startSQL():
    conn,cur = db()
    conn1,cur1 = db1()
    # 取出所有提醒
    sql = "select * from downosc group by stockid order by stockid;"
    cur1.execute(sql,())
    record = cur1.fetchall()
    stockid = [i[3] for i in record]
    allNotify = [[] for i in range(len(record))]
    
    sql = "select * from downosc order by stockid;"
    cur1.execute(sql,())
    record = cur1.fetchall()
    stockid1 = [i[3] for i in record]
    stockname = [i[4] for i in record]
    token = [i[2] for i in record]
    times = [i[5] for i in record]
    # 依股票代號分組
    for i in range(len(record)):
        allNotify[stockid.index(stockid1[i])].append([stockid1[i],stockname[i],token[i],times[i]])

    # 取出有設置提醒的股票的所有股價
    allPrice = []
    for i in range(len(stockid)):
        sql = "select * from stockprice1 where stockid = %s order by date;"
        cur.execute(sql,(stockid[i],))
        record1 = cur.fetchall()
        stockprice = [j[3] for j in record1]
        allPrice.append(stockprice)
    global done
    done = [True for i in range(len(stockid))]
    # 爬蟲抓取即時股價
    startSearch(stockid,allNotify,allPrice)
    for i in range(len(done)):
        # 如果已發過提醒
        if done[i] == False:
            # 刪除該提醒
            sql = "delete from downosc where stockid = %s;"
            cur1.execute(sql,(stockid[i],))
    conn1.commit()
    print("已完成刪除OSC已提醒動作！")
    

def main():
    while True:
        nowTime = datetime.datetime.now()
        nowTime1 = nowTime.strftime("%Y/%m/%d %H:%M:%S")
        hour = int(nowTime1[11]+nowTime1[12])
        minute = int(nowTime1[14]+nowTime1[15])
        second = int(nowTime1[17]+nowTime1[18])
        #if hour > 9:
        if (hour >= 9 and hour < 13) or (hour == 13 and minute <= 30):
            print("開始提醒服務！")
            while True:
                nowTime = datetime.datetime.now()
                nowTime1 = nowTime.strftime("%Y/%m/%d %H:%M:%S")
                hour = int(nowTime1[11]+nowTime1[12])
                minute = int(nowTime1[14]+nowTime1[15])
                second = int(nowTime1[17]+nowTime1[18])
                #if hour > 9:
                if (hour >= 9 and hour < 13) or (hour == 13 and minute <= 30):
                    print("從資料庫抓取資料...")
                    startSQL()
                    print("等待下一次抓取...")
                    time.sleep(6)
                else:
                    break
        else:
            if hour < 9:
                sleepTime = (9-hour-1)*60*60+(60-minute-1)*60+(60-second-1)
                print("等待開市, 還有"+str(9-hour-1)+"小時"+str(60-minute-1)+"分鐘"+str(60-second-1)+"秒")
                print(sleepTime)
                time.sleep(sleepTime)
            else:
                sleepTime = (9-hour-1+24)*60*60+(60-minute-1)*60+(60-second-1)
                print("等待開市, 還有"+str(9-hour-1+24)+"小時"+str(60-minute-1)+"分鐘"+str(60-second-1)+"秒")
                print(sleepTime)
                time.sleep(sleepTime)
main()