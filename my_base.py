import json
import mysql.connector
import pandas as pd
from sqlalchemy import create_engine
from urllib.parse import quote_plus

class My_base():
    def __init__(self):
        with open('C:/Users/mykola/Desktop/mySQLpithon/credentials.json') as f:
            self.cfg = json.load(f)

    def open(self):
        self.mydb = mysql.connector.connect(host=self.cfg['host'], user=self.cfg['user'], password=self.cfg['password'], database=self.cfg['db'])#, autocommit=True)
        self.cursor = self.mydb.cursor(buffered=True)

    def close(self):
        self.cursor.reset()
        self.cursor.close()
        self.mydb.close()
        

    def export_pd(self, data, name):
        df = pd.DataFrame(data)
        engine = create_engine('mysql+pymysql://mykola:%s@176.114.1.160/mykola' % quote_plus("M@nenk023"))
        # engine = create_engine("postgres://user:%s@host/database" % quote_plus("p@ss"))
        df.to_sql(name=name, con=engine, if_exists='replace')


def main():
    db = My_base()
    db.open()

    sql = "SELECT * FROM disks"

    db.cursor.execute(sql)
    for e in db.cursor.fetchall():
        print(e)

    x = [{'a':1, 'b':2}, {'a':3, 'b':4}]
    db.export_pd(x, 'test')

    db.close()

if __name__ == '__main__':
    main()