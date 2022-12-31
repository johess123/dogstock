# 找出跌破均線或設置價格的股票並發出提醒
import requests, json
import threading
import mysql.connector
from bs4 import BeautifulSoup
import concurrent.futures
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

def getPrice(nowPrice,stockid,stockname,tenprice,updown10,allToken,no,fiveprice,twentyprice,sixtyprice,updown5,updown20,updown60):
    if nowPrice != False:
        if done[no][1] == True:
            if nowPrice < tenprice and updown10 != "d": # 跌過十日且昨天不是跌過
                # 提醒跌過十日
                done[no][1] = False
                for i in range(len(allToken[1])):
                    token = allToken[1][i][0]
                    times = allToken[1][i][1]
                    message = '使用者您好，'+str(stockid)+" "+stockname+" 跌過十日！"
                    win32api.ShellExecute(0, "open", "test8.py", f"{token} {message} {times}", "", 0)
                message = '使用者您好，'+str(stockid)+" "+stockname+" 跌過十日！"
                print(message)
        if done[no][0] == True:
            if nowPrice < fiveprice and updown5 != "d": # 跌過五日且昨天不是跌過
                # 提醒跌過五日
                done[no][0] = False
                for i in range(len(allToken[0])):
                    token = allToken[0][i][0]
                    times = allToken[0][i][1]
                    message = '使用者您好，'+str(stockid)+" "+stockname+" 跌過五日！"
                    win32api.ShellExecute(0, "open", "test8.py", f"{token} {message} {times}", "", 0)
                message = '使用者您好，'+str(stockid)+" "+stockname+" 跌過五日！"
                print(message)
        if done[no][2] == True:
            if nowPrice < twentyprice and updown20 != "d": # 跌過二十日且昨天不是跌過
                # 提醒跌過二十日
                done[no][2] = False
                for i in range(len(allToken[2])):
                    token = allToken[2][i][0]
                    times = allToken[2][i][1]
                    message = '使用者您好，'+str(stockid)+" ："+stockname+" 跌過二十日！"
                    win32api.ShellExecute(0, "open", "test8.py", f"{token} {message} {times}", "", 0)
                message = '使用者您好，'+str(stockid)+" "+stockname+" 跌過二十日！"
                print(message)
        if done[no][3] == True:
            if nowPrice < sixtyprice and updown60 != "d": # 跌過六十日且昨天不是跌過
                # 提醒跌過六十日
                done[no][3] = False
                for i in range(len(allToken[3])):
                    token = allToken[3][i][0]
                    times = allToken[3][i][1]
                    message = '使用者您好，'+str(stockid)+" "+stockname+" 跌過六十日！"
                    win32api.ShellExecute(0, "open", "test8.py", f"{token} {message} {times}", "", 0)
                message = '使用者您好，'+str(stockid)+" "+stockname+" 跌過六十日！"
                print(message)
        # 提醒跌破設定價格
        for i in range(len(allToken[4])):
            if allToken[4][i][2] != False and nowPrice < allToken[4][i][1]:
                token = allToken[4][i][0]
                times = allToken[4][i][2]
                message = '使用者您好，'+str(stockid)+" "+stockname+" 跌過設定價格"+str(allToken[4][i][1])+"元！"
                print(message)
                win32api.ShellExecute(0, "open", "test8.py", f"{token} {message} {times}", "", 0)
                allToken[4][i][2] = False

def startSearch(stockid,stockname,tenprice,updown10,allToken,fiveprice,twentyprice,sixtyprice,updown5,updown20,updown60):
    print(allToken)
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
            for i in range(len(stockid)):
                if nowPrice[i] == []:
                    nowPrice[i] = False
            if len(nowPrice) == len(stockid):
                do = False
            else:
                print("資料未抓齊，重新抓取")
                time.sleep(1)
        except:
            print("連線失敗")
            time.sleep(1)
    no = [i for i in range(len(stockid))]
    #直譯
    #for i in range(len(stockid)):
        #getPrice(nowPrice[i],stockid[i],stockname[i],tenprice[i],updown10[i],allToken[i],i,fiveprice[i],twentyprice[i],sixtyprice[i],updown5[i],updown20[i],updown60[i])
    #多執行緒
    
    print(len(nowPrice),len(stockid),len(tenprice),len(updown10),len(allToken),len(no),len(fiveprice),len(twentyprice),len(sixtyprice),len(updown5),len(updown20),len(updown60))
    with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
        executor.map(getPrice,nowPrice,stockid,stockname,tenprice,updown10,allToken,no,fiveprice,twentyprice,sixtyprice,updown5,updown20,updown60)

def startSQL():
    conn,cur = db()
    conn1,cur1 = db1()
    # 列出所有股票代號、名稱
    sql = "SELECT * FROM `stockname1`;"
    cur.execute(sql,())
    record1 = cur.fetchall()
    stockid = [i[0] for i in record1] # 所有股票代號
    
    # 列出所有使用者均線提醒
    sql = "select * from downover;"
    cur1.execute(sql,())
    record = cur1.fetchall()
    
    # 根據股票代號分組提醒
    allNotify = [[[],[],[],[],[]] for i in range(len(stockid))]
    for i in range(len(record)):
        if record[i][3] == 5:
            allNotify[stockid.index(record[i][4])][0].append([record[i][2],record[i][6]])
        elif record[i][3] == 10:
            allNotify[stockid.index(record[i][4])][1].append([record[i][2],record[i][6]])
        elif record[i][3] == 20:
            allNotify[stockid.index(record[i][4])][2].append([record[i][2],record[i][6]])
        else:
            allNotify[stockid.index(record[i][4])][3].append([record[i][2],record[i][6]])
    
    # 列出所有使用者價錢提醒
    sql = "select token,stockid,price,time from downprice;"
    cur1.execute(sql,())
    result = cur1.fetchall()
    for i in range(len(result)):
        allNotify[stockid.index(result[i][1])][4].append([result[i][0],result[i][2]/100,result[i][3]])
    
    # 計算10日線臨界值
    sql = "select stockid,round(avg(price),2) from (SELECT ROW_NUMBER() OVER (PARTITION BY stockid order by date desc) row_num,stockid,price,date FROM stockprice1)a where row_num >= 1 and row_num <= 9 group by stockid;"
    cur.execute(sql,())
    record2 = cur.fetchall()
    tenprice = [i[1] for i in record2] # 所有股票十日線價格臨界值
    # 計算20日線臨界值
    sql = "select stockid,round(avg(price),2) from (SELECT ROW_NUMBER() OVER (PARTITION BY stockid order by date desc) row_num,stockid,price,date FROM stockprice1)a where row_num >= 1 and row_num <= 19 group by stockid;"
    cur.execute(sql,())
    record5 = cur.fetchall()
    twentyprice = [i[1] for i in record5] # 所有股票二十日線價格臨界值
    # 計算5日線臨界值
    sql = "select stockid,round(avg(price),2) from (SELECT ROW_NUMBER() OVER (PARTITION BY stockid order by date desc) row_num,stockid,price,date FROM stockprice1)a where row_num >= 1 and row_num <= 4 group by stockid;"
    cur.execute(sql,())
    record6 = cur.fetchall()
    fiveprice = [i[1] for i in record6] # 所有股票五日線價格臨界值
    # 計算60日線臨界值
    sql = "select stockid,round(avg(price),2) from (SELECT ROW_NUMBER() OVER (PARTITION BY stockid order by date desc) row_num,stockid,price,date FROM stockprice1)a where row_num >= 1 and row_num <= 59 group by stockid;"
    cur.execute(sql,())
    record7 = cur.fetchall()
    sixtyprice = [i[1] for i in record7] # 所有股票六十日線價格臨界值
    # 找出所有股票前一天各均線的漲跌情況
    sql = "select updown5,updown10,updown20,updown60 from updownover1"
    cur.execute(sql,())
    record3 = cur.fetchall()
    updown5 = [i[0] for i in record3] # 所有股票前一天的五日均線漲跌情況
    updown10 = [i[1] for i in record3] # 所有股票前一天的十日均線漲跌情況
    updown20 = [i[2] for i in record3] # 所有股票前一天的二十日均線漲跌情況
    updown60 = [i[3] for i in record3] # 所有股票前一天的六十日均線漲跌情況
    # 整理出使用者有設置提醒的股票
    sql = "select stockid,stockname from downover group by stockid;"
    cur1.execute(sql,())
    result = cur1.fetchall()
    sql = "select stockid,stockname from downprice group by stockid;"
    cur1.execute(sql,())
    result += cur1.fetchall()
    result = sorted(result)
    finalResult = []
    stockname = []
    for i in range(len(result)):
        if result[i][0] not in finalResult:
            finalResult.append(result[i][0])
            stockname.append(result[i][1])
    
    five = [[] for i in range(len(finalResult))]
    ten = [[] for i in range(len(finalResult))]
    twenty = [[] for i in range(len(finalResult))]
    sixty = [[] for i in range(len(finalResult))]
    ud5 = [[] for i in range(len(finalResult))]
    ud10 = [[] for i in range(len(finalResult))]
    ud20 = [[] for i in range(len(finalResult))]
    ud60 = [[] for i in range(len(finalResult))]
    finalDayNotify = []
    for i in range(len(stockid)):
        for j in range(len(finalResult)):
            if stockid[i] in finalResult[j]:
                five[j] = fiveprice[i]
                ten[j] = tenprice[i]
                twenty[j] = twentyprice[i]
                sixty[j] = sixtyprice[i]
                ud5[j] = updown5[i]
                ud10[j] = updown10[i]
                ud20[j] = updown20[i]
                ud60[j] = updown60[i]
        if allNotify[i] != [[],[],[],[],[]]:
            finalDayNotify.append(allNotify[i])
    print("資料已準備就緒！")
    global done
    done = [[True for i in range(4)] for j in range(len(finalResult))]
    startSearch(finalResult,stockname,ten,ud10,finalDayNotify,five,twenty,sixty,ud5,ud20,ud60)
    # 刪除今日已提醒的跌破提醒設置
    for i in range(len(done)): # 檢查所有股票跌破紀錄
        if done[i][0] == False: # 今日已跌破五日
            sql = "delete from downover where stockid = %s and day = 5;"
            cur1.execute(sql,(finalResult[i],))
        if done[i][1] == False: # 今日已跌破十日
            sql = "delete from downover where stockid = %s and day = 10;"
            cur1.execute(sql,(finalResult[i],))
        if done[i][2] == False: # 今日已跌破二十日
            sql = "delete from downover where stockid = %s and day = 20;"
            cur1.execute(sql,(finalResult[i],))
        if done[i][3] == False: # 今日已跌破六十日
            sql = "delete from downover where stockid = %s and day = 60;"
            cur1.execute(sql,(finalResult[i],))
        conn1.commit()
    print("已完成刪除均線已提醒動作！")
    # 刪除已提醒價格
    for i in range(len(finalResult)):
        for j in range(len(finalDayNotify[i][4])):
            if finalDayNotify[i][4][j][2] == False:
                sql = "delete from downprice where stockid = %s and token = %s and price = %s;"
                cur1.execute(sql,(finalResult[i],finalDayNotify[i][4][j][0],int(round(finalDayNotify[i][4][j][1]*100,2))))
            conn1.commit()
    print("已完成刪除價格已提醒動作！")

def main():
    while True:
        nowTime = datetime.datetime.now()
        nowTime1 = nowTime.strftime("%Y/%m/%d %H:%M:%S")
        hour = int(nowTime1[11]+nowTime1[12])
        minute = int(nowTime1[14]+nowTime1[15])
        second = int(nowTime1[17]+nowTime1[18])
        #if hour >= 9:
        if (hour >= 9 and hour < 13) or (hour == 13 and minute <= 30):
            print("開始提醒服務！")
            while True:
                nowTime = datetime.datetime.now()
                nowTime1 = nowTime.strftime("%Y/%m/%d %H:%M:%S")
                hour = int(nowTime1[11]+nowTime1[12])
                minute = int(nowTime1[14]+nowTime1[15])
                second = int(nowTime1[17]+nowTime1[18])
                #if hour >= 9:
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