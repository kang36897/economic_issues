# -*- coding: utf-8 -*-
from datetime import datetime
from os import path

from restaurant.Cook import Cook
from restaurant.Chain import Chain

from restaurant.DataSaver import CSVSaver, DBSaver
from restaurant.Restaurant import Restaurant

if __name__ == '__main__':
    start_time = datetime.now()
    print "begin to predict ........."

    target_signals = [u'DM8034', u'DM0066',u'CJM729']

    balance = 10000
    cpu_num = 5

    cook = Cook()
    cook.collectPotato(path.abspath("inputs/relations.xlsx"))
    cook.collectTomato(path.abspath("inputs/signals.xlsx"))
    cook.sortInvolvedSignals()


    csv_saver = CSVSaver(path.abspath("outputs"))

    # 1.create a database called 'investment'
    # 2.replace the parameters in brackets with your real arguments, such as your mysql database user name, password
    # 2.1 host -> 127.0.0.1, default port is 3306
    # db_saver = DBSaver('financial_predict',  'mysql+mysqlconnector://[user]:[pass]@[host]:[port]/[schema]', schema='investment')
    db_saver = DBSaver('financial_predict', 'mysql+mysqlconnector://investor:admin@127.0.0.1:3306/investment',
                       schema='investment')
    savers = [csv_saver, db_saver]

    chain = Chain(cook,cpu_num, balance)
    chain.doBusiness(target_signals, savers)

    time_elapsed = datetime.now() - start_time
    print "Prediction is completed ........."
    print 'Time elapsed (hh:mm:ss.ms) {}'.format(time_elapsed)

