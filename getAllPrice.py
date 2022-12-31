# 取得歷史股價
from dbConfig import conn,cur
from FinMind.data import DataLoader

api = DataLoader()
token1="eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJkYXRlIjoiMjAyMi0wNi0yOSAxNTozNjozMSIsInVzZXJfaWQiOiJLZW5ueTA2MTI1NiIsImlwIjoiMTIyLjI1NC4zMy4xODMifQ.Y2NtEY5j_6VmS2mUSeicB5luRVfenQsb2iY7YxbnR5Y"
token2="eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJkYXRlIjoiMjAyMi0wNy0xNyAxNDo0OTo0NyIsInVzZXJfaWQiOiJKb2hlc3MwNjEyNTYiLCJpcCI6IjEyMi4yNTQuMzMuMTgzIn0.C3mt3j0t7Nol2ZOL8wd-TPfw6dDnLmtzLXWhpVKcBuk"
api.login_by_token(api_token=token1)
uid1="Kenny061256"
uid2="Johess061256"
api.login(user_id=uid1,password='Kenny061256')

sql = "SELECT stockid FROM `stockname1`;"
cur.execute(sql,())
record1 = cur.fetchall()
for i in record1:
    if int(i[0]) > 6251:
        print(i[0])
        df = api.taiwan_stock_daily(
            stock_id=str(i[0]),
            start_date='2022-08-13',
            end_date='2022-08-30'
        )
        allQuantity = df['Trading_Volume'].tolist()
        allDate = df['date'].tolist()
        allPrice = df['close'].tolist()
        #a = input()
        #print(len(allDate))
        #print(len(allQuantity))
        for j in range(len(allQuantity)):
            sql = "INSERT INTO stockprice1(stockid,date,price,quantity) VALUES (%s,%s,%s,%s)"
            cur.execute(sql,(i[0],allDate[j],allPrice[j],allQuantity[j]))
        conn.commit()