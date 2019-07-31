from storage.mysql import Mysql
from config import *


class CreateTable():
    def __init__(self):
        self.tableName = TABLENAME
        self.create_table = '''
               CREATE TABLE {0} (
               id varchar(16) primary key ,
               rate varchar(128),
               title varchar(128),
               director varchar(128),
               writer varchar(256),
               actor varchar(1536),
               genre varchar(128),
               produce_area varchar(128),
               language varchar(128),
               release_date varchar(128),
               runtime varchar(16),
               alias varchar(512),
               imdb varchar(16),
               intro varchar(1536),
               voters varchar(16),
               five_stars varchar(16),
               four_stars varchar(16),
               three_stars varchar(16),
               two_stars varchar(16), 
               one_star varchar(16),
               collections varchar(16),
               wishes varchar(16)
               );
               '''.format(self.tableName)
        self.db = Mysql()

    def create(self):
        self.db.modify('drop table if exists {0};'.format(self.tableName))
        self.db.modify(self.create_table)
        self.db.close()
