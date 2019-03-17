# -*- coding: utf-8 -*-
from datetime import datetime
from os import path

from restaurant.Cook import Cook
from restaurant.Chain import Chain

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

    chain = Chain(cook,cpu_num, balance)
    chain.doBusiness(target_signals, path.abspath("outputs"))

    time_elapsed = datetime.now() - start_time
    print "Prediction is completed ........."
    print 'Time elapsed (hh:mm:ss.ms) {}'.format(time_elapsed)

