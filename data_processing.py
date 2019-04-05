# -*- coding: utf-8 -*-
from datetime import datetime
from os import path

from restaurant.Cook import Cook
from restaurant.Chain import Chain

from restaurant.DataSaver import CSVSaver, DBSaver
from restaurant.Sieve import Sieve

import json


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
    db_saver = DBSaver(mysql_config['table_name'],
                       'mysql+mysqlconnector://{}:{}@{}:{}/{}'.format(mysql_config['user'],mysql_config['pass'],
                                                                      mysql_config['host'], mysql_config['port'],
                                                                      mysql_config['schema']),
                       schema=mysql_config['schema'])


    savers = [csv_saver]

    filter = Sieve()
    chain = Chain(cook, config_data['cpu_num'], config_data['balance'])
    chain.doBusiness(config_data['target_signals'], savers, filter)

    time_elapsed = datetime.now() - start_time
    print "Prediction is completed ........."
    print 'Time elapsed (hh:mm:ss.ms) {}'.format(time_elapsed)
