# 取得今日股價
import requests, json
from dbConfig import conn, cur
from bs4 import BeautifulSoup
import datetime

def startSQL2():
    sql = "select stockid,round(avg(price),2) from (SELECT ROW_NUMBER() OVER (PARTITION BY stockid order by date desc) row_num,stockid,price,date FROM stockprice1)a where row_num >= 1 and row_num <= 5 group by stockid;"
    cur.execute(sql,())
    record1 = cur.fetchall()
    fiveprice = [i[1] for i in record1] # 所有股票五日線價格

    sql = "select stockid,round(avg(price),2) from (SELECT ROW_NUMBER() OVER (PARTITION BY stockid order by date desc) row_num,stockid,price,date FROM stockprice1)a where row_num >= 1 and row_num <= 10 group by stockid;"
    cur.execute(sql,())
    record2 = cur.fetchall()
    stockid = [i[0] for i in record2] # 所有股票id
    tenprice = [i[1] for i in record2] # 所有股票十日線價格
    
    sql = "select stockid,round(avg(price),2) from (SELECT ROW_NUMBER() OVER (PARTITION BY stockid order by date desc) row_num,stockid,price,date FROM stockprice1)a where row_num >= 1 and row_num <= 20 group by stockid;"
    cur.execute(sql,())
    record3 = cur.fetchall()
    twentyprice = [i[1] for i in record3] # 所有股票二十日線價格
    
    sql = "select stockid,round(avg(price),2) from (SELECT ROW_NUMBER() OVER (PARTITION BY stockid order by date desc) row_num,stockid,price,date FROM stockprice1)a where row_num >= 1 and row_num <= 60 group by stockid;"
    cur.execute(sql,())
    record4 = cur.fetchall()
    sixtyprice = [i[1] for i in record4] # 所有股票六十日線價格
    # 爬取及時股價
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
    nowPrice = []
    for i in range(len(idAndPrice)):
        if idAndPrice[i][0] in stockid:
            nowPrice.append(float(idAndPrice[i][1]))
    # 5日
    for i in range(len(stockid)):
        if nowPrice[i] > fiveprice[i]:
            sql = "UPDATE updownover1 SET updown5 = 'u' WHERE stockid = %s"
            cur.execute(sql,(stockid[i],))
            conn.commit()
        elif nowPrice[i] < fiveprice[i]:
            sql = "UPDATE updownover1 SET updown5 = 'd' WHERE stockid = %s"
            cur.execute(sql,(stockid[i],))
            conn.commit()
        else:
            sql = "UPDATE updownover1 SET updown5 = 'f' WHERE stockid = %s"
            cur.execute(sql,(stockid[i],))
            conn.commit()
    # 10日
    for i in range(len(stockid)):
        if nowPrice[i] > tenprice[i]:
            sql = "UPDATE updownover1 SET updown10 = 'u' WHERE stockid = %s"
            cur.execute(sql,(stockid[i],))
            conn.commit()
        elif nowPrice[i] < tenprice[i]:
            sql = "UPDATE updownover1 SET updown10 = 'd' WHERE stockid = %s"
            cur.execute(sql,(stockid[i],))
            conn.commit()
        else:
            sql = "UPDATE updownover1 SET updown10 = 'f' WHERE stockid = %s"
            cur.execute(sql,(stockid[i],))
            conn.commit()
    # 20日
    for i in range(len(stockid)):
        if nowPrice[i] > twentyprice[i]:
            sql = "UPDATE updownover1 SET updown20 = 'u' WHERE stockid = %s"
            cur.execute(sql,(stockid[i],))
            conn.commit()
        elif nowPrice[i] < twentyprice[i]:
            sql = "UPDATE updownover1 SET updown20 = 'd' WHERE stockid = %s"
            cur.execute(sql,(stockid[i],))
            conn.commit()
        else:
            sql = "UPDATE updownover1 SET updown20 = 'f' WHERE stockid = %s"
            cur.execute(sql,(stockid[i],))
            conn.commit()
    # 60日
    for i in range(len(stockid)):
        if nowPrice[i] > sixtyprice[i]:
            sql = "UPDATE updownover1 SET updown60 = 'u' WHERE stockid = %s"
            cur.execute(sql,(stockid[i],))
            conn.commit()
        elif nowPrice[i] < sixtyprice[i]:
            sql = "UPDATE updownover1 SET updown60 = 'd' WHERE stockid = %s"
            cur.execute(sql,(stockid[i],))
            conn.commit()
        else:
            sql = "UPDATE updownover1 SET updown60 = 'f' WHERE stockid = %s"
            cur.execute(sql,(stockid[i],))
            conn.commit()

def startSQL1():
    # 今天日期
    today = str(datetime.date.today())
    sql = "SELECT * FROM `stockname1`;"
    cur.execute(sql,())
    record1 = cur.fetchall()
    stockid = [i[0] for i in record1]
    # 爬股價
    url = "https://histock.tw/stock/rank.aspx?p=all"
    res = requests.get(url)
    soup = BeautifulSoup(res.text,'html.parser')
    stock = soup.find('table',id='CPHB1_gv')
    stock1 = stock.find_all('tr')
    idPriceQuantity = []
    for i in range(1,len(stock1)):
        stock2 = stock1[i].find_all('td')
        stockid1 = str(stock2[0].getText().replace('\n',"")) # 股票代號
        price = float(stock2[2].getText().replace('\n',"").replace(",","")) # 價格
        quantity = int(stock2[11].getText().replace('\n',"").replace(",",""))*1000 # 成交量
        idPriceQuantity.append([stockid1,price,quantity])
    idPriceQuantity = sorted(idPriceQuantity)
    idPriceQuantity1 = []
    for i in range(len(idPriceQuantity)):
        if idPriceQuantity[i][0] in stockid:
            idPriceQuantity1.append(idPriceQuantity[i])
    for i in range(len(stockid)):
        sql = "INSERT INTO stockprice1(stockid,date,price,quantity) values (%s,%s,%s,%s)"
        #cur.execute(sql,(stockid[i],today,idPriceQuantity1[i][1],idPriceQuantity1[i][2]))
        cur.execute(sql,(stockid[i],"2022-08-12",idPriceQuantity1[i][1],idPriceQuantity1[i][2]))
    conn.commit()

def main():
    startSQL1()
    startSQL2()
    # while True:
        # nowTime = datetime.datetime.now()
        # nowTime1 = nowTime.strftime("%Y/%m/%d %H:%M:%S")
        # hour = int(nowTime1[11]+nowTime1[12])
        # minute = int(nowTime1[14]+nowTime1[15])
        # second = int(nowTime1[17]+nowTime1[18])
        # if (hour >= 14):
            # print("開始儲存今日各股收盤價！")
            # #startSQL1()
            # print("開始儲存今日各股漲跌情況！")
            # startSQL2()
        # else:
            # if hour < 14:
                # sleepTime = (14-hour-1)*60*60+(60-minute-1)*60+(60-second-1)
                # print("等待收盤, 還有"+str(14-hour-1)+"小時"+str(60-minute-1)+"分鐘"+str(60-second-1)+"秒")
                # print(sleepTime)
                # time.sleep(sleepTime)
            # else:
                # sleepTime = (14-hour+24-1)*60*60+(60-minute-1)*60+(60-second-1)
                # print("等待收盤, 還有"+str(14-hour+24-1)+"小時"+str(60-minute-1)+"分鐘"+str(60-second-1)+"秒")
                # print(sleepTime)
                # time.sleep(sleepTime)
main()