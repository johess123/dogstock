# 找出跌破均線或設置價格的股票並發出提醒
import requests, json
import threading
from dbConfig import conn, cur
from dbConfig1 import conn1, cur1
from bs4 import BeautifulSoup
import concurrent.futures
import datetime
import time
import win32api

def getPrice(nowPrice,stockid,stockname,tenprice,updown10,allToken,no,fiveprice,twentyprice,sixtyprice,updown5,updown20,updown60,allUserPriceToken):
    if done[no][1] == True:
        if nowPrice < tenprice and updown10 != "d": # 跌過十日且昨天不是跌過
            # 提醒跌過十日
            done[no][1] = False
            for i in range(len(allToken[1])):
                token = allToken[1][i]
                message = '使用者您好，股票代號：'+str(stockid)+" 股票名稱："+stockname+" 已跌過十日均線！"
                print(message)
                win32api.ShellExecute(0, "open", "test7.py", f"{token} {message}", "", 0)
            message = '使用者您好，股票代號：'+str(stockid)+" 股票名稱："+stockname+" 已跌過十日均線！"
            print(message)
    if done[no][0] == True:
        if nowPrice < fiveprice and updown5 != "d": # 跌過五日且昨天不是跌過
            # 提醒跌過五日
            done[no][0] = False
            for i in range(len(allToken[0])):
                token = allToken[0][i]
                message = '使用者您好，股票代號：'+str(stockid)+" 股票名稱："+stockname+" 已跌過五日均線！"
                print(message)
                win32api.ShellExecute(0, "open", "test7.py", f"{token} {message}", "", 0)
            message = '使用者您好，股票代號：'+str(stockid)+" 股票名稱："+stockname+" 已跌過五日均線！"
            print(message)
    if done[no][2] == True:
        if nowPrice < twentyprice and updown20 != "d": # 跌過二十日且昨天不是跌過
            # 提醒跌過二十日
            done[no][2] = False
            for i in range(len(allToken[2])):
                token = allToken[2][i]
                message = '使用者您好，股票代號：'+str(stockid)+" 股票名稱："+stockname+" 已跌過二十日均線！"
                print(message)
                win32api.ShellExecute(0, "open", "test7.py", f"{token} {message}", "", 0)
            message = '使用者您好，股票代號：'+str(stockid)+" 股票名稱："+stockname+" 已跌過二十日均線！"
            print(message)
    if done[no][3] == True:
        if nowPrice < sixtyprice and updown60 != "d": # 跌過六十日且昨天不是跌過
            # 提醒跌過六十日
            done[no][3] = False
            for i in range(len(allToken[3])):
                token = allToken[3][i]
                message = '使用者您好，股票代號：'+str(stockid)+" 股票名稱："+stockname+" 已跌過六十日均線！"
                print(message)
                win32api.ShellExecute(0, "open", "test7.py", f"{token} {message}", "", 0)
            message = '使用者您好，股票代號：'+str(stockid)+" 股票名稱："+stockname+" 已跌過六十日均線！"
            print(message)
    # 提醒跌破設定價格
    for i in range(len(allUserPriceToken)):
        if nowPrice < allUserPriceToken[i][1]:
            token = allUserPriceToken[i][0]
            message = '使用者您好，股票代號：'+str(stockid)+" 股票名稱："+stockname+" 已跌過您設定的價格"+str(allUserPriceToken[i][1])+"元！"
            print(message)
            deletePrice.append([stockid,token,allUserPriceToken[i][1]])
            win32api.ShellExecute(0, "open", "test7.py", f"{token} {message}", "", 0)

def startSearch(stockid,stockname,tenprice,updown10,record1,allToken,fiveprice,twentyprice,sixtyprice,updown5,updown20,updown60,allUserPriceToken):
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
    no = [i for i in range(1764)]
    ###
    #直譯
    #for i in range(len(stockid)):
        #getPrice(nowPrice[i],stockid[i],stockname[i],tenprice[i],updown10[i],allToken[i],i,fiveprice[i],twentyprice[i],sixtyprice[i],updown5[i],updown20[i],updown60[i])
    ###
    ###
    global deletePrice
    deletePrice = []
    ###
    #多執行緒
    print(len(nowPrice),len(stockid),len(stockname),len(tenprice),len(updown10),len(allToken),len(no),len(fiveprice),len(twentyprice),len(sixtyprice),len(updown5),len(updown20),len(updown60),len(allUserPriceToken))
    with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
        executor.map(getPrice,nowPrice,stockid,stockname,tenprice,updown10,allToken,no,fiveprice,twentyprice,sixtyprice,updown5,updown20,updown60,allUserPriceToken)
    print(deletePrice)
    # 刪除已提醒價格
    for i in range(len(deletePrice)):
        sql = "delete from downprice where stockid = %s and token = %s and price = %s;"
        cur1.execute(sql,(deletePrice[i][0],deletePrice[i][1],int(round(deletePrice[i][2]*100,2))))
    conn1.commit()

def startSQL():
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
    # 找出所有股票對應的token
    allToken = [[[],[],[],[]] for i in range(1764)]
    sql = "select token,stockid,day from downover;"
    cur1.execute(sql,())
    result = cur1.fetchall()
    for i in range(len(result)):
        if result[i][2] == 5:
            allToken[stockid.index(result[i][1])][0].append(result[i][0])
        elif result[i][2] == 10:
            allToken[stockid.index(result[i][1])][1].append(result[i][0])
        elif result[i][2] == 20:
            allToken[stockid.index(result[i][1])][2].append(result[i][0])
        else:
            allToken[stockid.index(result[i][1])][3].append(result[i][0])
    ###
    allUserPriceToken = [[] for i in range(1764)]
    sql = "select token,stockid,price from downprice;"
    cur1.execute(sql,())
    result = cur1.fetchall()
    for i in range(len(result)):
        allUserPriceToken[stockid.index(result[i][1])].append([result[i][0],result[i][2]/100])
    print(allUserPriceToken)
    ###
    print("資料已準備就緒！")
    while True:
        nowTime = datetime.datetime.now()
        nowTime1 = nowTime.strftime("%Y/%m/%d %H:%M:%S")
        hour = int(nowTime1[11]+nowTime1[12])
        minute = int(nowTime1[14]+nowTime1[15])
        second = int(nowTime1[17]+nowTime1[18])
        if (hour >= 9 and hour < 13) or (hour == 13 and minute <= 30):
            startSearch(stockid,stockname,tenprice,updown10,record1,allToken,fiveprice,twentyprice,sixtyprice,updown5,updown20,updown60,allUserPriceToken)
            print("等待下一次抓取...")
            #print(done)
            time.sleep(10)
        else:
            #startSearch(stockid,stockname,tenprice,updown10,record1,allToken,fiveprice,twentyprice,sixtyprice,updown5,updown20,updown60,allUserPriceToken)
            #print("等待下一次抓取...")
            #print(done)
            #time.sleep(10)
            # 刪除今日已提醒的跌破提醒設置
            for i in range(len(done)): # 檢查所有股票跌破紀錄
                if done[i][0] == False: # 今日已跌破五日
                    sql = "delete from downover where stockid = %s and day = 5;"
                    cur1.execute(sql,(stockid[i],))
                if done[i][1] == False: # 今日已跌破十日
                    sql = "delete from downover where stockid = %s and day = 10;"
                    cur1.execute(sql,(stockid[i],))
                if done[i][2] == False: # 今日已跌破二十日
                    sql = "delete from downover where stockid = %s and day = 20;"
                    cur1.execute(sql,(stockid[i],))
                if done[i][3] == False: # 今日已跌破六十日
                    sql = "delete from downover where stockid = %s and day = 60;"
                    cur1.execute(sql,(stockid[i],))
                conn1.commit()
            print("已完成今日刪除已提醒動作！")
            break

def main():
    while True:
        nowTime = datetime.datetime.now()
        nowTime1 = nowTime.strftime("%Y/%m/%d %H:%M:%S")
        hour = int(nowTime1[11]+nowTime1[12])
        minute = int(nowTime1[14]+nowTime1[15])
        second = int(nowTime1[17]+nowTime1[18])
        if (hour >= 9 and hour < 13) or (hour == 13 and minute <= 30):
            global done
            done = [[True for i in range(4)] for j in range(1764)]
            print("開始提醒服務！")
            startSQL()
        else:
            #global done
            #done = [[True for i in range(4)] for j in range(1764)]
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