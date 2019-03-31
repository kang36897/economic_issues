# -*- coding: utf-8 -*-

from unittest import TestCase
from restaurant.DataSaver import DBSaver
import pandas as pd

class DataSaverTest(TestCase):

    def test_save_db(self):
        df = pd.DataFrame(data= [[0.1, 0.21]], columns=['a', 'b'])
        print df

        db_saver =  DBSaver('test_db_saver',  'mysql+mysqlconnector://investor:admin@127.0.0.1:3306/investment', schema='investment', reaction_if_table_exist="replace")
        db_saver.save(df)