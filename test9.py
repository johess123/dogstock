from dbConfig import conn, cur
from dbConfig1 import conn1,cur1
sql = "select * from stockname1;"
cur.execute(sql,())
record = cur.fetchall()
for i in range(len(record)):
    sql = "INSERT INTO stockname (stockid,stockname) VALUES (%s,%s);"
    cur1.execute(sql,(record[i][0],record[i][1]))
conn1.commit();