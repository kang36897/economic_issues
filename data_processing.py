# -*- coding: utf-8 -*-
import json
from datetime import datetime
from os import path

from restaurant.Chain import Chain
from restaurant.Cook import Cook
from restaurant.DataSaver import CSVSaver, DBSaver
import pandas as pd

def keep_material_in_place(input_files):
    for item in input_files:
        if not path.exists(item.encode("utf-8")):
            raise Exception("File: {} not exists".format(item))

    return True


def load_config(config_json):
    with open(config_json, "r") as config:
        return json.load(config, encoding="utf-8")

def selectPossibleTimes(row, target_signals):
    if row.signal in target_signals:
        return True

    return False

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

    if "filter" in config_data and "relevance" in config_data["filter"]:
        cook.identifyPoisonMushroom(config_data["filter"]["relevance"])

    target_criteria = config_data['target_signals']
    target_signals = cook.pickUpTargetSignals(target_criteria)
    print 'Target signals:\n {}\n'.format(target_signals)

    print 'Possible Times:'
    columns = cook.getPossibleTimes().keys()
    possibleTimes = cook.getPossibleTimes();
    pt = pd.DataFrame(data= [ possibleTimes[key] for key in possibleTimes.keys()], index= [key for key in columns],
                      columns = ['possible_times'])
    pt.index.name = 'signal'
    pt = pt.reset_index().sort_values(by=['possible_times'], ascending=False)
    print pt[pt.apply(selectPossibleTimes, axis=1, args=(target_signals,))]
    print '\n'

    print 'Poison Mushroom:'
    pm = pd.DataFrame(data = cook.getPoisionMushroom(), columns=['LEFT', 'RIGHT'])
    count, _ = pm.shape
    if count == 0:
        print 'There are no poison mushrooms'
    else:
        print pm
    print '\n'

    csv_saver = CSVSaver(path.abspath("outputs"))

    # 1.create a database called 'investment'
    # 2.replace the parameters in brackets with your real arguments, such as your mysql database user name, password
    # 2.1 host -> 127.0.0.1, default port is 3306
    # db_saver = DBSaver('financial_predict',  'mysql+mysqlconnector://[user]:[pass]@[host]:[port]/[schema]', schema='investment')
    db_config = config_data["db_config"]
    db_saver = DBSaver.createSaver(db_config, cook.getInvolvedSignals())

    savers = [db_saver]

    draftSieve = None
    drawback_ratio = None
    exp_return_ratio = None
    sharp_ratio = None
    pl_ratio = None
    max_active_num = None

    if 'filter' in config_data:
        drawback_ratio = None if 'drawback_ratio' not in config_data['filter'] else config_data['filter'][
            'drawback_ratio']
        exp_return_ratio = None if 'exp_return_ratio' not in config_data['filter'] else config_data['filter'][
            'exp_return_ratio']
        sharp_ratio = None if 'sharp_ratio' not in config_data['filter'] else config_data['filter']['sharp_ratio']
        pl_ratio = None if 'pl_ratio' not in config_data['filter'] else config_data['filter']['pl_ratio']
        max_active_num = None if 'max_active_num' not in config_data['filter'] else config_data['filter'][
            'max_active_num']

    draftSieve = (drawback_ratio,exp_return_ratio,sharp_ratio,pl_ratio, max_active_num, cook.getPoisionMushroom())



    chain = Chain(cook, config_data['cpu_num'], config_data['balance'])
    chain.doBusiness(target_signals, savers, draftSieve)

    time_elapsed = datetime.now() - start_time
    print "Prediction is completed ........."
    print 'Time elapsed (hh:mm:ss.ms) {}'.format(time_elapsed)
