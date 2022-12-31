# 連線資料庫
import mysql.connector

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