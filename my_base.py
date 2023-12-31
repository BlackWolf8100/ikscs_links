import json
import mysql.connector
import pandas as pd
from sqlalchemy import create_engine
from urllib.parse import quote_plus
import sqlite3

class My_base():
    def __init__(self, dbfile = None, logger = False):
        if dbfile == None: 
            with open('c:\\API\Mykola\ikscs_links\credentials_MK.json') as f:
                self.cfg = json.load(f)
        self.dbfile = dbfile
        self.logger = logger
        
    def log(self, mesage):
        if not self.logger:
            return
        self.logger.log(mesage)

    def open(self):
        if self.dbfile == None:
            try:
                self.mydb = mysql.connector.connect(host=self.cfg['host'], user=self.cfg['user'], password=self.cfg['password'], database=self.cfg['db'])#, autocommit=True)
                self.cursor = self.mydb.cursor(buffered=True)
            except Exception as eror:
                print(eror)
                return False
        else:
            self.mydb = sqlite3.connect(self.dbfile)
            self.cursor = self.mydb.cursor()
        return True
    
    def get_one_table(self, sql):
        try:
            self.cursor.execute(sql)
            result = [e[0] for e in self.cursor.fetchall()]
        except Exception as eror:
            print(eror)
            return []
        return result
    
    def execute(self, sql, values = None):
        sql = self.change_sql(sql)
        try:
            if values:
                self.cursor.execute(sql, values)
            else:
                self.cursor.execute(sql)
            self.mydb.commit()
        except Exception as eror:
            print(eror)
            self.log(str(eror))
            self.log(sql)
            
    def executemany(self, sql, values):
        sql = self.change_sql(sql)
        try:
            self.cursor.executemany(sql, values)
            self.mydb.commit()
        except Exception as eror:
            print(eror)
            self.log(str(eror))
            self.log(sql)
    
    def change_sql(self, sql):
        if self.dbfile:
            sql = sql.replace('%s', '?')
            sql = sql.replace('INSERT IGNORE', 'INSERT OR IGNORE')
        return sql
    
    def close(self):
        if not self.dbfile:
            self.cursor.reset()
        self.cursor.close()
        self.mydb.close()
        

    # def export_pd(self, data, name):
    #     df = pd.DataFrame(data)
    #     engine = create_engine('mysql+pymysql://**********:%s@************' % quote_plus("*******"))
    #     # engine = create_engine("postgres://user:%s@host/database" % quote_plus("p@ss"))
    #     df.to_sql(name=name, con=engine, if_exists='replace')


def main():
    db = My_base(dbfile = 'test.db')
    db.open()

    sql = "SELECT * FROM disks"
    
    db.cursor.execute(sql)
    for e in db.cursor.fetchall():
        print(e)

    sql = f'INSERT OR IGNORE INTO disks (Field1, Field2) VALUES (?, ?)'
    values = (456, 789)
    db.cursor.execute(sql, values)
    db.mydb.commit()
    # x = [{'a':1, 'b':2}, {'a':3, 'b':4}]
    # db.mydb.export_pd(x, 'test')

    db.close()

if __name__ == '__main__':
    main()