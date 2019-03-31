# -*- coding: utf-8 -*-
import mysql.connector
from sqlalchemy import create_engine
from abc import ABCMeta
from os import path


class Saver:

    def __init__(self):
        pass

    def save(self, df, desired_signals = None):
        pass


class CSVSaver(Saver):
    def __init__(self, delivery_path, encoding="utf-8", float_format="%.2f"):
        Saver.__init__(self)

        self.default_delivery_path = delivery_path
        self.default_encoding = encoding
        self.default_float_format = float_format

    def save(self, df, desired_signals = None):
        csv_file = path.join(self.default_delivery_path, "{}.csv".format("+".join(desired_signals)))
        df.to_csv(csv_file, encoding=self.default_encoding,
                  float_format=self.default_float_format)


class DBSaver(Saver):

    def __init__(self, table_name, connection, schema=None, column_dtype=None, reaction_if_table_exist="append"):
        """
        :param table_name:
        :param connection: 'mysql+mysqlconnector://[user]:[pass]@[host]:[port]/[schema]'
        :param schema:
        :param column_dtype:
        :param reaction_if_table_exist:
        """

        Saver.__init__(self)

        self.table_name = table_name
        self.database_connection = connection
        self.schema = None
        self.column_dtype = column_dtype
        self.reaction = reaction_if_table_exist

    def save(self, df, desired_signals = None):
        engine = create_engine(self.database_connection, echo=False)
        df.to_sql(name=self.table_name, con=engine, if_exists=self.reaction, index=False)
