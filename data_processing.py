# -*- coding: utf-8 -*-
import json
from datetime import datetime
from os import path

from sqlalchemy.types import Float, Integer

from restaurant.Chain import Chain
from restaurant.Cook import Cook
from restaurant.DataSaver import CSVSaver, DBSaver
from restaurant.Sieve import Sieve


def keep_material_in_place(input_files):
    for item in input_files:
        if not path.exists(item.encode("utf-8")):
            raise Exception("File: {} not exists".format(item))

    return True


def load_config(config_json):
    with open(config_json, "r") as config:
        return json.load(config, encoding="utf-8")


if __name__ == '__main__':
    start_time = datetime.now()
    print "begin to predict ........."

    config_file = path.join(path.abspath("config"), "config.json")
    keep_material_in_place([config_file])

    input_directory = path.abspath("inputs")
    config_data = load_config(config_file)

    cook = Cook()
    cook.collectPotato(path.join(input_directory, config_data["relation_file"]))
    cook.collectTomato(path.join(input_directory, config_data["signal_file"]))
    cook.sortInvolvedSignals()

    csv_saver = CSVSaver(path.abspath("outputs"))

    # 1.create a database called 'investment'
    # 2.replace the parameters in brackets with your real arguments, such as your mysql database user name, password
    # 2.1 host -> 127.0.0.1, default port is 3306
    # db_saver = DBSaver('financial_predict',  'mysql+mysqlconnector://[user]:[pass]@[host]:[port]/[schema]', schema='investment')
    mysql_config = config_data["mysql"]
    column_type = {
        'balance': Integer(),
        'corelation': Float(precision=2, asdecimal=True, decimal_return_scale=2),
        'times': Float(precision=2, asdecimal=True, decimal_return_scale=2),
        'drawback': Float(precision=2, asdecimal=True, decimal_return_scale=2),
        'exp_profit': Float(precision=2, asdecimal=True, decimal_return_scale=2),
        'drawback%': Float(precision=2, asdecimal=True, decimal_return_scale=2),
        'exp_profit%': Float(precision=2, asdecimal=True, decimal_return_scale=2),
        'sharp%': Float(precision=2, asdecimal=True, decimal_return_scale=2),
        'pl%': Float(precision=2, asdecimal=True, decimal_return_scale=2)
    }
    for item in cook.getInvolvedSignals():
        column_type[item] = Float(precision=2, asdecimal=True, decimal_return_scale=2)

    db_saver = DBSaver(mysql_config['table_name'],
                       'mysql+mysqlconnector://{}:{}@{}:{}/{}'.format(mysql_config['user'], mysql_config['pass'],
                                                                      mysql_config['host'], mysql_config['port'],
                                                                      mysql_config['schema']),
                       schema=mysql_config['schema'],
                       column_dtype=column_type
                       )

    savers = [csv_saver]

    sieve = None
    if 'filter' in config_data:
        drawback_ratio = None if 'drawback_ratio' not in config_data['filter'] else config_data['filter'][
            'drawback_ratio']
        exp_return_ratio = None if 'exp_return_ratio' not in config_data['filter'] else config_data['filter'][
            'exp_return_ratio']
        sharp_ratio = None if 'sharp_ratio' not in config_data['filter'] else config_data['filter']['sharp_ratio']
        pl_ratio = None if 'pl_ratio' not in config_data['filter'] else config_data['filter']['pl_ratio']
        sieve = Sieve(drawback_ratio=drawback_ratio, exp_return_ratio=exp_return_ratio,
                      sharp_ratio=sharp_ratio, pl_ratio=pl_ratio)
    else:
        sieve = Sieve()

    chain = Chain(cook, config_data['cpu_num'], config_data['balance'])
    chain.doBusiness(config_data['target_signals'], savers, sieve)

    time_elapsed = datetime.now() - start_time
    print "Prediction is completed ........."
    print 'Time elapsed (hh:mm:ss.ms) {}'.format(time_elapsed)
