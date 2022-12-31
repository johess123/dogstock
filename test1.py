# 找出漲過均線名單並儲存至資料庫
import mysql.connector
import requests, json
import threading
from bs4 import BeautifulSoup
import concurrent.futures
import datetime
import time

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

def getPrice(nowPrice,stockid,stockname,tenprice,updown10,last10,no,fiveprice,twentyprice,sixtyprice,last5,last20,last60,updown5,updown20,updown60):
    if nowPrice != False:
        if done[no][2] == True:
            if nowPrice > tenprice and updown10 != "u" and nowPrice > last10: # 漲過十日且昨天不是漲過且斜率為正
                done[no][2] = False
                overStock.append([stockid,stockname,10,1])
                #sql = "INSERT INTO upover (stockid,stockname,day,slope) VALUES (%s,%s,%s,%s);"
                #cur1.execute(sql,(stockid,stockname,10,1))
                #conn1.commit()
        if done[no][3] == True:
            if nowPrice > tenprice and updown10 != "u": # 漲過十日且昨天不是漲過但不考慮斜率為正
                done[no][3] = False
                overStock.append([stockid,stockname,10,0])
                #sql = "INSERT INTO upover (stockid,stockname,day,slope) VALUES (%s,%s,%s,%s);"
                #cur1.execute(sql,(stockid,stockname,10,0))
                #conn1.commit()
        if done[no][0] == True:
            if nowPrice > fiveprice and updown5 != "u" and nowPrice > last5: # 漲過五日均線且昨天不是漲過且斜率為正
                done[no][0] = False
                overStock.append([stockid,stockname,5,1])
                #sql = "INSERT INTO upover (stockid,stockname,day,slope) VALUES (%s,%s,%s,%s);"
                #cur1.execute(sql,(stockid,stockname,5,1))
                #conn1.commit()
        if done[no][1] == True:
            if nowPrice > fiveprice and updown5 != "u": # 漲過五日均線且昨天不是漲過但不考慮斜率為正
                done[no][1] = False
                overStock.append([stockid,stockname,5,0])
                #sql = "INSERT INTO upover (stockid,stockname,day,slope) VALUES (%s,%s,%s,%s);"
                #cur1.execute(sql,(stockid,stockname,5,0))
                #conn1.commit()
        if done[no][4] == True:
            if nowPrice > twentyprice and updown20 != "u" and nowPrice > last20: # 漲過二十日均線且昨天不是漲過且斜率為正
                done[no][4] = False
                overStock.append([stockid,stockname,20,1])
                #sql = "INSERT INTO upover (stockid,stockname,day,slope) VALUES (%s,%s,%s,%s);"
                #cur1.execute(sql,(stockid,stockname,20,1))
                #conn1.commit()
        if done[no][5] == True:
            if nowPrice > twentyprice and updown20 != "u": # 漲過二十日均線且昨天不是漲過但不考慮斜率為正
                done[no][5] = False
                overStock.append([stockid,stockname,20,0])
                #sql = "INSERT INTO upover (stockid,stockname,day,slope) VALUES (%s,%s,%s,%s);"
                #cur1.execute(sql,(stockid,stockname,20,0))
                #conn1.commit()
        if done[no][6] == True:
            if nowPrice > sixtyprice and updown60 != "u" and nowPrice > last60: # 漲過六十日均線且昨天不是漲過且斜率為正
                done[no][6] = False
                overStock.append([stockid,stockname,60,1])
                #sql = "INSERT INTO upover (stockid,stockname,day,slope) VALUES (%s,%s,%s,%s);"
                #cur1.execute(sql,(stockid,stockname,60,1))
                #conn1.commit()
        if done[no][7] == True:
            if nowPrice > sixtyprice and updown60 != "u": # 漲過六十日均線且昨天不是漲過但不考慮斜率為正
                done[no][7] = False
                overStock.append([stockid,stockname,60,0])
                #sql = "INSERT INTO upover (stockid,stockname,day,slope) VALUES (%s,%s,%s,%s);"
                #cur1.execute(sql,(stockid,stockname,60,0))
                #conn1.commit()

def startSearch(stockid,stockname,tenprice,updown10,last10,fiveprice,twentyprice,sixtyprice,last5,last20,last60,updown5,updown20,updown60):
    # 爬取及時股價
    do = True
    while do == True:
        try:
            nowPrice = [[] for i in range(len(stockid))]
            ###
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
                    #nowPrice.append(float(idAndPrice[i][1]))
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
    print(len(nowPrice),len(stockid),len(stockname),len(tenprice),len(updown10),len(last10),len(fiveprice),len(twentyprice),len(sixtyprice),len(last5),len(last20),len(last60),len(updown5),len(updown20),len(updown60))
    for i in range(len(stockid)):
        getPrice(nowPrice[i],stockid[i],stockname[i],tenprice[i],updown10[i],last10[i],i,fiveprice[i],twentyprice[i],sixtyprice[i],last5[i],last20[i],last60[i],updown5[i],updown20[i],updown60[i])

def startSQL():
    conn,cur = db()
    conn1,cur1 = db1()
    # 清空昨日新增的漲過均線
    sql = "Delete from upover where 1;"
    # sql = "TRUNCATE TABLE `stock`.`upover`;"
    cur1.execute(sql,())
    conn1.commit()
    # 列出所有股票ID、名稱
    sql = "SELECT * FROM `stockname1`;"
    cur.execute(sql,())
    record1 = cur.fetchall()
    stockname = [i[1] for i in record1]
    # 計算10日線
    sql = "select stockid,round(avg(price),2) from (SELECT ROW_NUMBER() OVER (PARTITION BY stockid order by date desc) row_num,stockid,price,date FROM stockprice1)a where row_num >= 1 and row_num <= 9 group by stockid;"
    cur.execute(sql,())
    record2 = cur.fetchall()
    stockid = [i[0] for i in record2] # 所有股票id
    tenprice = [i[1] for i in record2] # 所有股票十日線價格
    # 計算20日線
    sql = "select stockid,round(avg(price),2) from (SELECT ROW_NUMBER() OVER (PARTITION BY stockid order by date desc) row_num,stockid,price,date FROM stockprice1)a where row_num >= 1 and row_num <= 19 group by stockid;"
    cur.execute(sql,())
    record5 = cur.fetchall()
    twentyprice = [i[1] for i in record5] # 所有股票二十日線價格
    # 計算5日線
    sql = "select stockid,round(avg(price),2) from (SELECT ROW_NUMBER() OVER (PARTITION BY stockid order by date desc) row_num,stockid,price,date FROM stockprice1)a where row_num >= 1 and row_num <= 4 group by stockid;"
    cur.execute(sql,())
    record6 = cur.fetchall()
    fiveprice = [i[1] for i in record6] # 所有股票五日線價格
    # 計算60日線
    sql = "select stockid,round(avg(price),2) from (SELECT ROW_NUMBER() OVER (PARTITION BY stockid order by date desc) row_num,stockid,price,date FROM stockprice1)a where row_num >= 1 and row_num <= 59 group by stockid;"
    cur.execute(sql,())
    record7 = cur.fetchall()
    sixtyprice = [i[1] for i in record7] # 所有股票六十日線價格
    # 找出所有股票前一天各均線的漲跌情況
    sql = "select updown5,updown10,updown20,updown60 from updownover1"
    cur.execute(sql,())
    record3 = cur.fetchall()
    updown5 = [i[0] for i in record3] # 所有股票前一天的五日均線漲跌情況
    updown10 = [i[1] for i in record3] # 所有股票前一天的十日均線漲跌情況
    updown20 = [i[2] for i in record3] # 所有股票前一天的二十日均線漲跌情況
    updown60 = [i[3] for i in record3] # 所有股票前一天的六十日均線漲跌情況
    # 每支股票五天前股價
    sql = "select price from (SELECT ROW_NUMBER() OVER (PARTITION BY stockid order by date desc) row_num,stockid,price FROM stockprice1)a where row_num = 4;"
    cur.execute(sql,())
    record4 = cur.fetchall()
    last5 = [i[0] for i in record4]
    # 每支股票十天前股價
    sql = "select price from (SELECT ROW_NUMBER() OVER (PARTITION BY stockid order by date desc) row_num,stockid,price FROM stockprice1)a where row_num = 9;"
    cur.execute(sql,())
    record4 = cur.fetchall()
    last10 = [i[0] for i in record4]
    # 每支股票二十天前股價
    sql = "select stockid,price from (SELECT ROW_NUMBER() OVER (PARTITION BY stockid order by date desc) row_num,stockid,price FROM stockprice1)a where row_num = 19;"
    cur.execute(sql,())
    record4 = cur.fetchall()
    last20 = [[] for i in range(len(stockid))]
    # 不滿20筆資料的股票取最後那一天的
    for i in range(len(record4)):
        last20[stockid.index(record4[i][0])] = record4[i][1]
    sql = "select stockid,price from (SELECT ROW_NUMBER() OVER (PARTITION BY stockid order by date) row_num,stockid,price FROM stockprice1)a where row_num = 1;"
    cur.execute(sql,())
    record4 = cur.fetchall()
    for i in range(len(last20)):
        if last20[i] == []:
            last20[i] = record4[i][1]
    # 每支股票六十天前股價
    sql = "select stockid,price from (SELECT ROW_NUMBER() OVER (PARTITION BY stockid order by date desc) row_num,stockid,price FROM stockprice1)a where row_num = 59;"
    cur.execute(sql,())
    record4 = cur.fetchall()
    last60 = [[] for i in range(len(stockid))]
    # 不滿60筆資料的股票取最後那一天的
    for i in range(len(record4)):
        last60[stockid.index(record4[i][0])] = record4[i][1]
    sql = "select stockid,price from (SELECT ROW_NUMBER() OVER (PARTITION BY stockid order by date) row_num,stockid,price FROM stockprice1)a where row_num = 1;"
    cur.execute(sql,())
    record4 = cur.fetchall()
    for i in range(len(last60)):
        if last60[i] == []:
            last60[i] = record4[i][1]
    print("資料已準備就緒！")
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
            startSearch(stockid,stockname,tenprice,updown10,last10,fiveprice,twentyprice,sixtyprice,last5,last20,last60,updown5,updown20,updown60)
            for i in range(len(overStock)):
                sql = "INSERT INTO upover (stockid,stockname,day,slope) VALUES (%s,%s,%s,%s);"
                cur1.execute(sql,(overStock[i][0],overStock[i][1],overStock[i][2],overStock[i][3]))
            conn1.commit()
            print("儲存完畢！")
            print("等待下一次抓取...")
            #print(done)
            time.sleep(6)
        else:
            #startSearch(stockid,stockname,tenprice,updown10,last10,fiveprice,twentyprice,sixtyprice,last5,last20,last60,updown5,updown20,updown60)
            #print("等待下一次抓取...")
            #print(done)
            #time.sleep(10)
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
            global done
            done = [[True for i in range(8)] for j in range(1764)]
            print("開始提醒服務！")
            startSQL()
        else:
            #global done
            #done = [[True for i in range(8)] for j in range(1764)]
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