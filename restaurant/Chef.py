# -*- coding: utf-8 -*-

import pandas as pd
import math
from datetime import  datetime

def calculate_covariance(row, names_of_signals, standard_deviation_of_signals, relationship):
    temp_products = {}
    total = 0
    for s in names_of_signals:
        p = row[s] * standard_deviation_of_signals[s]

        temp_products[s] = p
        total += pow(p, 2)

    length = len(names_of_signals)
    for i in range(0, length - 1):
        left = names_of_signals[i]
        for j in range(i + 1, length):
            right = names_of_signals[j]
            # print "left: {}, right:{}".format(left, right)
            total += temp_products[left] * temp_products[right] * 2 * relationship[(left, right)]
    return math.sqrt(total)

def calculate_return(row, names_of_signals, expected_return_of_signals):
    total = 0

    for s in names_of_signals:
        total += row[s] * expected_return_of_signals[s]

    return total

def calculate_multiple(row):
    if row["corelation"] == 0:
        return 0

    return row["balance"] / row["corelation"]

def calculate_sharp_rate(row):
    if row["corelation"] == 0:
        return 0
    return (row["exp_return"] * 12 - row["balance"] * 0.05) / row["corelation"]


class Chef:
    def __init__(self, queue,
                 names_of_signals = None,
                 standard_deviation_of_signals = None,
                 expected_return_of_signals = None,
                 net_withdrawal_of_signals = None,
                 relation = None,
                 balance = None):
        self.__queue = queue
        self.__base_dish = None

        self.desiredFavor = names_of_signals
        self.standardDeviationOfSignals =standard_deviation_of_signals
        self.garlic = net_withdrawal_of_signals
        self.ginger = expected_return_of_signals
        self.salt = relation
        self.meat = balance
        pass

    def sliceTomato(self, st):
        df = pd.DataFrame(data=st.generateFrame(), columns=st.column_names)
        df = df.round(decimals=2)

        if not st.isDone():
            self.__queue.put(st.generateNextTask())

        self.__base_dish = df
        # print self.__base_dish
        return df


    def stir(self):
        self.__base_dish['balance'] = self.meat
        self.__base_dish["corelation"] = self.__base_dish.apply(calculate_covariance, axis=1,
                                                  args=(
                                                  self.desiredFavor, self.standardDeviationOfSignals, self.salt))
        self.__base_dish["drawback"] = self.__base_dish.apply(calculate_covariance, axis=1,
                                                args=(self.desiredFavor, self.garlic, self.salt))

        self.__base_dish["exp_return"] = self.__base_dish.apply(calculate_return, axis=1,
                                                  args=(self.desiredFavor, self.ginger))
        self.__base_dish['times'] = self.__base_dish.apply(calculate_multiple, axis=1)

        self.__base_dish["drawback_ratio"] = self.__base_dish["drawback"] * 100 / self.__base_dish["balance"]
        self.__base_dish["exp_return_ratio"] = self.__base_dish["exp_return"] * 100 / self.__base_dish["balance"]

        self.__base_dish = self.__base_dish.round({"drawback_ratio": 2, "exp_return_ratio": 2})

        self.__base_dish["sharp_ratio"] = self.__base_dish.apply(calculate_sharp_rate, axis=1)
        return self.__base_dish

    def doSpecialDish(self,st):
        start_time = datetime.now()
        self.sliceTomato(st)
        self.stir()

        time_elapsed = datetime.now() - start_time
        print 'doSpecialDish()-> Time elapsed (hh:mm:ss.ms) {}'.format(time_elapsed)
        print st.reportProgress()
        pass


    def handleOrder(self,st):
        self.doSpecialDish(st)
        return self.__base_dish