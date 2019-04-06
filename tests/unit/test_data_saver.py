# -*- coding: utf-8 -*-

from unittest import TestCase
from restaurant.DataSaver import DBSaver
import pandas as pd
from sqlalchemy import create_engine


class DataSaverTest(TestCase):

    def setUp(self):
        self.table_name = 'test_db_saver'
        self.schema = "investment"
        self.mysql_url = 'mysql+mysqlconnector://investor:admin@127.0.0.1:3306/{}'.format(self.schema)
        self.sql_engine = create_engine(self.mysql_url)

    def test_save_db(self):

        df = pd.DataFrame(data=[[0.1, 0.21]], columns=['a', 'b'])
        print df
        self.db_saver = DBSaver(self.table_name,
                                self.mysql_url,
                                schema=self.schema, reaction_if_table_exist="replace")
        self.db_saver.save(df)

    def tearDown(self):
        self.sql_engine.execute('DELETE from {} '.format(self.table_name))
