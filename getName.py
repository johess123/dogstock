# 取得所有股票名稱
from dbConfig import conn, cur

def main():
    f = open("name.txt",encoding="utf-8")
    for line in f.readlines():
        a = line.split(" ")
        b = []
        for i in a:
            i = i.replace('\n',"").replace('\t',"").replace('*',"").replace('#',"")
            if i != '':
                b.append(i)
        for i in range(0,len(b),2):
            stockid = b[i]
            stockName = b[i+1][:-1]
            sql = "INSERT INTO stockname(stockid,stockname) values (%s,%s)"
            cur.execute(sql,(stockid,stockName))
    conn.commit()
    f.close
    
main()