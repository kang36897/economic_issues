# -*- coding: utf-8 -*-

import numpy as np
import pandas as pd
from itertools import ifilter

from SmallTask import SmallTask
from copy import copy
import math


class Cook:

    def __init__(self):
        self.__relationship = None
        self.signalsInRelation = None
        self.relationOfSignals = None

        self.__signal_info = None
        self.involvedSignals = None

        self.standardDeviationOfSignals = None
        self.expectedReturnOfSignals = None
        self.netWithdrawalOfSignals = None
        self.possibleTimes = None
        self.referencesOfSignals = None

    def getSignalsInRelation(self):
        return self.signalsInRelation

    def collectRelationMaterial(self, inputFile):
        df = pd.read_excel(inputFile, sheet_name=0, index_col=0)
        df.fillna(0, inplace=True)
        df.replace(np.inf, 0)
        df = df.round(4)
        self.__relationship = df.astype(np.float32)
        return self.__relationship

    def checkSymmetryOfRelation(self):
        columns = list(self.__relationship.columns)
        indexes = list(self.__relationship.index)

        if len(columns) != len(indexes):
            return False

        for item in columns:
            if item not in indexes:
                return False
        else:
            return True

    def collectPotato(self, inputFile):
        self.collectRelationMaterial(inputFile)

        if self.checkSymmetryOfRelation():
            self.signalsInRelation = self.__relationship.columns
        else:
            raise Exception("The symmetry of data is not correct")

        self.relationOfSignals = {}
        for row in list(self.__relationship.index):
            for column in self.signalsInRelation:
                v = self.__relationship.loc[row, column]
                self.relationOfSignals[(row, column)] = v
                self.relationOfSignals[(column, row)] = v

    def getRelationOfSignal(self):
        return self.relationOfSignals

    def getInvolvedSignals(self):
        return self.involvedSignals

    def collectTomato(self, inputFile):
        df = pd.read_excel(inputFile, sheet_name=0, na_values=['-', '#N/A', 'NaN'])
        # delete unneeded columns
        if u'备注' in df.columns:
            del df[u'备注']

        self.__signal_info = df.fillna(0)

        self.involvedSignals = df[u'信号名称'].to_list()

        self.__signal_info = df.copy()[[u'信号名称', u'标准差', u'预期回报', u'净值回撤', u'最小手数', u'测试倍数']]
        # print self.__signal_info
        self.__signal_info = self.__signal_info.round(decimals={
            u'标准差': 4,
            u'预期回报': 4,
            u'净值回撤': 4,
            u'最小手数': 2
        })

        self.__signal_info = self.__signal_info.set_index(u'信号名称')

        self.standardDeviationOfSignals = {}
        for item in self.involvedSignals:
            self.standardDeviationOfSignals[item] = self.__signal_info.loc[item, u'标准差']

        self.expectedReturnOfSignals = {}
        for item in self.involvedSignals:
            self.expectedReturnOfSignals[item] = self.__signal_info.loc[item, u'预期回报']

        self.netWithdrawalOfSignals = {}
        for item in self.involvedSignals:
            self.netWithdrawalOfSignals[item] = self.__signal_info.loc[item, u'净值回撤']

        self.possibleTimes = {}
        self.referencesOfSignals = {}

        for item in self.involvedSignals:
            self.possibleTimes[item] = self.__signal_info.loc[item, u'最小手数'] * self.__signal_info.loc[
                item, u'测试倍数']

            self.referencesOfSignals[item] = self.__signal_info.loc[item, u'最小手数']

    def checkReferencesIsAboveZero(self, target_signals):
        for item in target_signals:
            if self.referencesOfSignals[item] <= 0:
                raise Exception("signal:{} -> 最小手数 <= 0".format(item))

    def checkDesiredSignalsIsAvailable(self, target_signals):
        available_signals = self.getAvailableSignals()
        if available_signals is None:
            raise Exception("Available Signals is None, you need to call collectPotato() and collectTomato() firstly ")

        for item in target_signals:
            if item not in available_signals:
                raise Exception("singal:{} is not an available signal,"
                                " it means that {} is neither in singals table nor relations table".format(item, item))

    def getStandardDeviationOfSignals(self):
        return self.standardDeviationOfSignals

    def getAvailableSignals(self):

        if self.involvedSignals is None or self.signalsInRelation is None:
            return None

        availableSignals = []

        for item in self.involvedSignals:
            if item in self.signalsInRelation:
                availableSignals.append(item)

        return availableSignals

    def getExpectedReturnOfSignals(self):
        return self.expectedReturnOfSignals

    def getNetWithdrawalOfSignals(self):
        return self.netWithdrawalOfSignals

    def getPossibleTimes(self):
        return self.possibleTimes

    def setPossibleTimes(self, possibleTimes):
        self.possibleTimes = possibleTimes

    def pickUrgentOrder(self, possible_times, desired_signals):
        max = 0
        result = None
        for key, value in possible_times.items():
            if value >= max and key in desired_signals:
                result = (key, value)
                max = value
        else:
            return result

    def receiveOrders(self, tablesInCall):
        (urgentTable, _) = self.pickUrgentOrder(self.getPossibleTimes(), tablesInCall)
        dishes = self.describeDishes(self.getPossibleTimes())

        normalTable = [x for x in ifilter(lambda x: x != urgentTable, tablesInCall)]

        urgentDish = dishes[urgentTable]

        sequence = []
        for n in range(len(urgentDish)):
            tables = copy(normalTable)
            tables.insert(0, urgentTable)

            dishesForTable = copy([dishes[t] for t in normalTable])
            dishesForTable.insert(0, [urgentDish[n]])

            st = SmallTask(n, tables, dishesForTable, start=0)
            sequence.append(st)

        return sequence

    def describeDishes(self, possible_times):
        dishes = {}
        for key, value in possible_times.items():
            dishes[key] = [x for x in np.arange(0.0, value, step=np.float64(0.01), dtype=np.float64)]
            dishes[key].append(value)
        return dishes

    def loadPlate(self, dish, filter):
        filtered_df = dish.loc[dish.apply(filter, axis=1), :]
        return filtered_df

    def sortInvolvedSignals(self):
        if self.involvedSignals is None:
            return self.involvedSignals

        self.involvedSignals.sort()
        return self.involvedSignals

    def getReferencesOfSignals(self):
        return self.referencesOfSignals
