# -*- coding: utf-8 -*-
from os import path

from sqlalchemy import create_engine, Integer, Float


class Saver:

    def __init__(self):
        pass

    def save(self, df, desired_signals=None):
        pass


class CSVSaver(Saver):
    def __init__(self, delivery_path, encoding="utf-8", float_format="%.2f"):
        Saver.__init__(self)

        self.default_delivery_path = delivery_path
        self.default_encoding = encoding
        self.default_float_format = float_format

    def save(self, df, desired_signals=None):
        csv_file = path.join(self.default_delivery_path, "{}.csv".format("+".join(desired_signals)))
        df.to_csv(csv_file, encoding=self.default_encoding,
                  float_format=self.default_float_format, index=False)


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
        self.schema = schema
        self.column_dtype = column_dtype
        self.reaction = reaction_if_table_exist

    def save(self, df, desired_signals=None):
        engine = create_engine(self.database_connection, echo=False)
        rounded_df = df.round(2)
        rounded_df.to_sql(name=self.table_name, con=engine, if_exists=self.reaction, index=False,
                          dtype=self.column_dtype, chunksize=3000)

    @staticmethod
    def createSaver(db_config, involved_signals):
        column_type = {
            'active_num':Integer(),
            'balance': Integer(),
            'covariance': Float(precision=2, asdecimal=True, decimal_return_scale=2),
            'times': Float(precision=2, asdecimal=True, decimal_return_scale=2),
            'drawback': Float(precision=2, asdecimal=True, decimal_return_scale=2),
            'exp_profit': Float(precision=2, asdecimal=True, decimal_return_scale=2),
            'drawback%': Float(precision=2, asdecimal=True, decimal_return_scale=2),
            'exp_profit%': Float(precision=2, asdecimal=True, decimal_return_scale=2),
            'sharp%': Float(precision=2, asdecimal=True, decimal_return_scale=2),
            'pl%': Float(precision=2, asdecimal=True, decimal_return_scale=2)
        }

        for item in involved_signals:
            column_type[item] = Float(precision=2, asdecimal=True, decimal_return_scale=2)

        if db_config['db_type'] == 'mysql':
            return DBSaver(db_config['table_name'],
                    'mysql+mysqlconnector://{}:{}@{}:{}/{}'.format(db_config['user'], db_config['pass'],
                                                                   db_config['host'], db_config['port'],
                                                                   db_config['schema']),
                    schema=db_config['schema'],
                    column_dtype=column_type
                    )
        else:
            return DBSaver(db_config['table_name'],
                           'mssql+pymssql://{}:{}@{}:{}/{}?charset=utf8'.format(db_config['user'], db_config['pass'],
                                                                          db_config['host'], db_config['port'],
                                                                          db_config['schema']),
                           schema=db_config['schema'],
                           column_dtype=column_type
                           )