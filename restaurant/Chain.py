# -*- coding: utf-8 -*-
from itertools import combinations

from restaurant.Restaurant import Restaurant
class Chain:

    def iteratePossiblePackage(self, target_signals):
        result = []
        for i in range(1, len(target_signals) + 1, 1):
            for item in combinations(target_signals, i):
                result.append(list(item))

        return result

    def __init__(self, cook, cpu_num, balance):
        self.cook = cook
        self.cpu_num = cpu_num
        self.balance = balance

    def doBusiness(self, target_signals, data_savers, filter):
        self.cook.checkReferencesIsAboveZero(target_signals)

        for item in self.iteratePossiblePackage(target_signals):
            restaurant = Restaurant(self.cpu_num, self.cook, self.balance)
            restaurant.setFilter(filter)
            restaurant.serveCustomer(item, data_savers)
