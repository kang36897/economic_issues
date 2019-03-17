# -*- coding: utf-8 -*-
from itertools import combinations

from restaurant.Restaurant import Restaurant
from os import path


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

    def doBusiness(self, target_signals, delivery_path):
        for item in self.iteratePossiblePackage(target_signals):
            restaurant = Restaurant(self.cpu_num, self.cook, self.balance)
            restaurant.serveCustomer(item, delivery_path=delivery_path)
