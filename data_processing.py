# -*- coding: utf-8 -*-
from datetime import datetime
from os import path

from restaurant.Cook import Cook

from restaurant.Restaurant import Restaurant

if __name__ == '__main__':
    start_time = datetime.now()
    print "begin to predict ........."

    balance = 5813.88
    cpu_num = 2
    desired_signals = [u'DM8034']

    cook = Cook()
    cook.collectPotato(path.abspath("inputs/relations.xlsx"))
    cook.collectTomato(path.abspath("inputs/signals.xlsx"))

    restaurant = Restaurant(cpu_num, cook, balance)
    restaurant.serveCustomer(desired_signals, delivery_path=path.abspath("outputs"))

    time_elapsed = datetime.now() - start_time
    print "Prediction is completed ........."
    print 'Time elapsed (hh:mm:ss.ms) {}'.format(time_elapsed)

