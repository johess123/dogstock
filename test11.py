# 儲存漲過OSC
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

def countOsc(stockid,stockname,allPrice,no):
    if done[no] == True:
        # 計算OSC
        df = pd.DataFrame(allPrice,columns = ["Close"])
        dif,macd,osc = talib.MACD(df['Close'],12,26,9)
        osc = osc.tolist()
        # 如果今日OSC大於昨日OSC，且昨天OSC小於前天OSC
        if osc[-1] > osc[-2]:
            done[no] = False
            # 存入漲過OSC(不考慮是否為首次)
            overStock.append([stockid,stockname,0])
            if osc[-2] < osc[-3]:
                # 存入漲過OSC(首次漲過)
                overStock.append([stockid,stockname,1])

def startSearch(stockid,stockname,allPrice):
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
            countOsc(stockid[i],stockname[i],allPrice[i],i)

def startSQL():
    conn,cur = db()
    conn1,cur1 = db1()
    # 清空昨日新增的漲過OSC
    #sql = "TRUNCATE TABLE `stock`.`macd`;"
    sql = "Delete from macd where 1;"
    cur1.execute(sql,())
    conn1.commit()
    # 取出所有股票的代號和名稱
    sql = "select * from stockname1;"
    cur.execute(sql,())
    record = cur.fetchall()
    stockid = [i[0] for i in record]
    stockname = [i[1] for i in record]
    
    # 取出所有股價
    allPrice = [[] for i in range(len(stockid))]
    sql = "select stockid,price from stockprice1 order by date;"
    cur.execute(sql,())
    record1 = cur.fetchall()
    for i in range(len(record1)):
        allPrice[stockid.index(record1[i][0])].append(record1[i][1])
    global done
    done = [True for i in range(len(stockid))]
    # 爬蟲抓取即時股價
    while True:
        nowTime = datetime.datetime.now()
        nowTime1 = nowTime.strftime("%Y/%m/%d %H:%M:%S")
        hour = int(nowTime1[11]+nowTime1[12])
        minute = int(nowTime1[14]+nowTime1[15])
        second = int(nowTime1[17]+nowTime1[18])
        #if hour > 9:
        if (hour < 13) or (hour == 13 and minute <= 30):
            # 新增漲過名單
            global overStock
            overStock = []
            startSearch(stockid,stockname,allPrice)
            # 儲存漲過OSC
            for i in range(len(overStock)):
                sql = "INSERT INTO macd (stockid,stockname,first) VALUES (%s,%s,%s);"
                cur1.execute(sql,(overStock[i][0],overStock[i][1],overStock[i][2]))
            conn1.commit()
            print("儲存完畢！")
            print("等待下一次抓取...")
            time.sleep(6)
        else:
            #startSearch(stockid,stockname,allPrice)
            #print("等待下一次抓取...")
            #time.sleep(10)
            print("已完成今日儲存漲過OSC動作！")
            break
    

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
            startSQL()
        else:
            #print("開始提醒服務！")
            #startSQL()
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