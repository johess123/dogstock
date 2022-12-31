import requests, json
from dbConfig import conn,cur
from bs4 import BeautifulSoup
import time

def main():
    url = "https://tw.stock.yahoo.com/quote/2409"
    res = requests.get(url)
    soup = BeautifulSoup(res.text,'html.parser')
    price = soup.find('span', class_="Fz(32px) Fw(b) Lh(1) Mend(16px) D(f) Ai(c) C($c-trend-down)")
    if price == None:
        price = soup.find('span', class_="Fz(32px) Fw(b) Lh(1) Mend(16px) D(f) Ai(c) C($c-trend-up)")
    print(float(price.getText()))
main()