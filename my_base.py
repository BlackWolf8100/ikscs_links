import json
import mysql.connector
import pandas as pd
from sqlalchemy import create_engine
from urllib.parse import quote_plus

class My_base():
    def __init__(self):
        with open('credentials.json') as f:
            self.cfg = json.load(f)

    def open(self):
        try:
            self.mydb = mysql.connector.connect(host=self.cfg['host'], user=self.cfg['user'], password=self.cfg['password'], database=self.cfg['db'])#, autocommit=True)
            self.cursor = self.mydb.cursor(buffered=True)
        except Exception as eror:
            print(eror)
            return False
        return True
    
    def get_one_table(self, sql):
        try:
            self.cursor.execute(sql)
            result = [e[0] for e in self.cursor.fetchall()]
        except Exception as eror:
            print(eror)
            return []
        return result
    
    def close(self):
        self.cursor.reset()
        self.cursor.close()
        self.mydb.close()
        

    # def export_pd(self, data, name):
    #     df = pd.DataFrame(data)
    #     engine = create_engine('mysql+pymysql://**********:%s@************' % quote_plus("*******"))
    #     # engine = create_engine("postgres://user:%s@host/database" % quote_plus("p@ss"))
    #     df.to_sql(name=name, con=engine, if_exists='replace')


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