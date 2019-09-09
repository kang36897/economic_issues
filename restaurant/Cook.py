# -*- coding: utf-8 -*-

import numpy as np
import pandas as pd


from restaurant.SmallTask import SmallTask, calculate_record_size
from copy import copy
import math


def calculateMultiple(row, risk_ratio, balance = 10000):
    key = row[u'信号名称']
    if row[u'最小手数'] == 0:
        return 0

    if row[u'净值回撤'] == 0:
        return 0

    if  risk_ratio is None or key not in risk_ratio:
        return 0

    return math.floor(math.fabs((balance * risk_ratio[key] * 1.0 / 100) / (row[u'净值回撤'] / (row[u'最小手数'] / 0.01))))


def calculateHistory(row):
    return round((row[u'检查日期'] - row[u'运行开始']).days / 30.5)


def pickUpBasedOn(row, criteria):
    return row['history'] > criteria[u'history'] and abs(row[u'方差倍数']) < criteria[u'variance']


class Condiment:
    def __init__(self, maxlots=None, minlots=None, steplength=None):
        self.maxlots = maxlots if maxlots is not None else 10
        self.minlots = minlots if minlots is not None else 0.00
        self.steplength = steplength if steplength is not None else 0.01


class InnerIterable:

    def __init__(self, normalTable, urgentTable, dishes):
        self.normalTable = normalTable
        self.urgentTable = urgentTable
        self.dishes = dishes
        self.urgentDish = self.dishes[self.urgentTable]

        default_page_size = 100 * 1000
        record_size_per_task = calculate_record_size([self.dishes[t] for t in self.normalTable])
        page_size = min(default_page_size, record_size_per_task)
        self.itemCount = len(self.urgentDish) * int(math.ceil(record_size_per_task / page_size))

    def getSize(self):
        return self.itemCount

    def __iter__(self):

        for n in range(len(self.urgentDish)):
            tables = copy(self.normalTable)
            tables.insert(0, self.urgentTable)

            dishesForTable = copy([self.dishes[t] for t in self.normalTable])
            dishesForTable.insert(0, [self.urgentDish[n]])

            st = SmallTask(n, tables, dishesForTable, start=0)

            for item in st:
                st.isSeed = False
                yield item


class Cook:

    def __init__(self):
        self.__relationship = None
        self.signalsInRelation = None
        self.relationOfSignals = None
        self.poisonMushroom = []

        self.__signal_info = None
        self.involvedSignals = None

        self.standardDeviationOfSignals = None
        self.expectedReturnOfSignals = None
        self.netWithdrawalOfSignals = None
        self.possibleTimes = None
        self.referencesOfSignals = None
        self.targetSignals = None
        self.condiment = None

    def collect(self, condiment):
        self.condiment = condiment

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

    def identifyPoisonMushroom(self, relevance):
        for (key, value) in self.relationOfSignals.items():
            if value > relevance:
                left, right = key
                if left == right or ((right, left) in self.poisonMushroom):
                    continue

                self.poisonMushroom.append(key)

    def getPoisionMushroom(self):
        return self.poisonMushroom

    def getRelationOfSignal(self):
        return self.relationOfSignals

    def getInvolvedSignals(self):
        return self.involvedSignals

    def collectTomato(self, inputFile, risk_ratio = None, balance = 10000):
        df = pd.read_excel(inputFile, sheet_name=0, na_values=['-', '#N/A', 'NaN'])
        # delete unneeded columns
        if u'备注' in df.columns:
            del df[u'备注']

        self.__signal_info = df.fillna(0)

        self.involvedSignals = self.__signal_info[u'信号名称'].to_list()

        self.__signal_info[u'history'] = self.__signal_info.apply(calculateHistory, axis=1)
        self.__signal_info = self.__signal_info.copy()[
            [u'信号名称', u'标准差', u'预期回报', u'净值回撤', u'最小手数', u'测试倍数', u'history', u'方差倍数']]

        # print self.__signal_info
        self.__signal_info = self.__signal_info.round(decimals={
            u'标准差': 4,
            u'预期回报': 4,
            u'净值回撤': 4,
            u'最小手数': 2
        })

        self.__signal_info[u'测试倍数'] = self.__signal_info.apply(calculateMultiple, axis=1, args=(risk_ratio, balance))

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

    def pickUpTargetSignals(self, criteria):
        if self.targetSignals is not None:
            return self.targetSignals

        temp = self.__signal_info[self.__signal_info.apply(pickUpBasedOn, axis=1, args=(criteria,))]
        self.targetSignals = list(temp.index)
        return self.targetSignals

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

        normalTable = [x for x in filter(lambda x: x != urgentTable, tablesInCall)]

        return InnerIterable(normalTable, urgentTable, dishes)

    def describeDishes(self, possible_times):
        dishes = {}
        for key, value in possible_times.items():
            start = max(0.0, self.condiment.minlots)
            stop = min(value, self.condiment.maxlots)
            dishes[key] = [np.around(x, decimals=2) for x in np.arange(start, stop,
                                                                       step=np.float64(self.condiment.steplength),
                                                                       dtype=np.float64)]
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
