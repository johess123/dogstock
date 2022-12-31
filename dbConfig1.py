# 連線資料庫
import mysql.connector

try:
    conn1 = mysql.connector.connect(
        user="kenny",
        password="Kenny061256",
        host="122.254.33.183",
        port=1440,
        database="stock"
    )
except:
    print("connection db error")
    exit(1)
cur1=conn1.cursor()