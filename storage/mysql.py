import pymysql
from config import *


class Mysql:
    def __init__(self):
        self.connect()

    @classmethod
    def connect(cls):
        cls.conn = pymysql.connect(
            host=MYSQL_HOST,
            db=MYSQL_DATABASE,
            user=MYSQL_USER,
            passwd=MYSQL_PASSWORD,
            port=MYSQL_PORT,
            charset='utf8'
        )
        cls.cursor = cls.conn.cursor()

    # 增删改
    def modify(self, sql, params=None):
        try:
            self.cursor.execute(sql, params)
            self.conn.commit()
        except Exception as e:
            print(e)
            self.conn.rollback()

    def modify_many(self, sql, params=None):
        try:
            self.cursor.executemany(sql, params)
            # self.conn.commit()
        except Exception as e:
            print(e)
            self.conn.rollback()

    # 查
    def fetchall(self, sql, params=None):
        self.cursor.execute(sql, params)
        return self.cursor.fetchall()

    def fetchone(self, sql, params=None):
        self.cursor.execute(sql, params)
        return self.cursor.fetchone()

    def fetchmany(self, sql, params=None, n=None):
        self.cursor.execute(sql, params)
        return self.cursor.fetchmany(n)

    def commit(self):
        self.conn.commit()

    def close(self):
        # self.conn.commit()
        self.cursor.close()
        self.conn.close()

    # def __enter__(self):
    #     self.conn = self.connect()
    #     self.cursor = self.conn.cursor()
    #     return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()


if __name__ == '__main__':
    db = Mysql()
    a = db.fetchone('select * from fu_zhou')
    print(a)
