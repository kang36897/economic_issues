# -*- coding: utf-8 -*-
from Cook import Cook
from os import path
from Restaurant import Restaurant

from datetime import  datetime

if __name__ == '__main__':
    start_time = datetime.now()
    print "begin to predict ........."

    balance = 15083
    cpu_num = 5
    desired_signals = [u'DM0066', u'CJM995']

    cook = Cook()
    cook.collectPotato(path.abspath("inputs/relations.xlsx"))
    cook.collectTomato(path.abspath("inputs/signals.xlsx"))

    restaurant = Restaurant(cpu_num, cook, balance)
    restaurant.serveCustomer(desired_signals)

    time_elapsed = datetime.now() - start_time
    print "Prediction is completed ........."
    print 'Time elapsed (hh:mm:ss.ms) {}'.format(time_elapsed)

