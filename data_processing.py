# -*- coding: utf-8 -*-
import json
from datetime import datetime
from os import path

from restaurant.Chain import Chain
from restaurant.Cook import Cook
from restaurant.DataSaver import CSVSaver, DBSaver


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

    input_directory = path.abspath("inputs")
    config_file = path.join(input_directory, "config.json")
    keep_material_in_place([config_file])

    config_data = load_config(config_file)

    cook = Cook()
    cook.collectPotato(path.join(input_directory, config_data["relation_file"]))
    cook.collectTomato(path.join(input_directory, config_data["signal_file"]),
                       config_data['risk_ratio'], config_data['balance'])
    cook.sortInvolvedSignals()

    csv_saver = CSVSaver(path.abspath("outputs"))

    # 1.create a database called 'investment'
    # 2.replace the parameters in brackets with your real arguments, such as your mysql database user name, password
    # 2.1 host -> 127.0.0.1, default port is 3306
    # db_saver = DBSaver('financial_predict',  'mysql+mysqlconnector://[user]:[pass]@[host]:[port]/[schema]', schema='investment')
    db_config = config_data["db_config"]
    db_saver = DBSaver.createSaver(db_config, cook.getInvolvedSignals())

    savers = [csv_saver]

    draftSieve = None
    if 'filter' in config_data:
        drawback_ratio = None if 'drawback_ratio' not in config_data['filter'] else config_data['filter'][
            'drawback_ratio']
        exp_return_ratio = None if 'exp_return_ratio' not in config_data['filter'] else config_data['filter'][
            'exp_return_ratio']
        sharp_ratio = None if 'sharp_ratio' not in config_data['filter'] else config_data['filter']['sharp_ratio']
        pl_ratio = None if 'pl_ratio' not in config_data['filter'] else config_data['filter']['pl_ratio']
        max_active_num = None if 'max_active_num' not in config_data['filter'] else config_data['filter'][
            'max_active_num']
        draftSieve = (drawback_ratio,exp_return_ratio,sharp_ratio,pl_ratio, max_active_num)


    chain = Chain(cook, config_data['cpu_num'], config_data['balance'])
    chain.doBusiness(config_data['target_signals'], savers, draftSieve)

    time_elapsed = datetime.now() - start_time
    print "Prediction is completed ........."
    print 'Time elapsed (hh:mm:ss.ms) {}'.format(time_elapsed)
