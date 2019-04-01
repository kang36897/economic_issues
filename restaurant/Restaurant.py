# -*- coding: utf-8 -*-
from multiprocessing import Pool, Manager, Lock

from Chef import Chef
import pandas as pd
from os import path
from copy import copy


def wrapper(chef, st):
    return chef.handleOrder(st)


class Restaurant:

    def __init__(self, cpu_num, servant, balance):
        self.cpu_num = cpu_num
        self.p = Pool(cpu_num)
        self.m = Manager()
        self.servant = servant

        self.queue = self.m.Queue()
        self.balance = balance
        self.task_lock = Lock()

        self.filter = None

    def getTaskLock(self):
        return self.task_lock

    def compensate(self, df, desired_signals, full_signals):
        shared_columns = []

        for item in df.columns:
            if item not in desired_signals:
                shared_columns.append(item)

        for item in full_signals:
            if item not in desired_signals:
                df[item] = 0

        ideal_columns = copy(full_signals)
        ideal_columns.extend(shared_columns)

        return df[ideal_columns]

    def serveCustomer(self, desired_signals, data_savers):
        taskSequence = self.servant.receiveOrders(desired_signals)

        for st in taskSequence:
            self.queue.put(st)

        async_result_set = []
        result_set = []
        while True:

            if len(async_result_set) < self.cpu_num:
                st = self.queue.get()
                chef = Chef(self.queue,
                            names_of_signals=desired_signals,
                            references_of_signals=self.servant.getReferencesOfSignals(),
                            standard_deviation_of_signals=self.servant.getStandardDeviationOfSignals(),
                            expected_return_of_signals=self.servant.getExpectedReturnOfSignals(),
                            net_withdrawal_of_signals=self.servant.getNetWithdrawalOfSignals(),
                            relation=self.servant.getRelationOfSignal(),
                            balance=self.balance
                            )
                chef.setColumnsInRedefinedOrder(
                    [u'balance', u'corelation', u'times', u'drawback', u'drawback%', u'exp_profit', u'exp_profit%',
                     u'sharp%', u'pl%'])
                temp = self.p.apply_async(wrapper, (chef, st))
                async_result_set.append(temp)

            else:

                ar = async_result_set.pop(0)

                dish = ar.get()
                result_set.append(dish)
            if self.queue.empty():
                break

        self.p.close()
        self.p.join()

        result_set.extend([r.get() for r in async_result_set])

        dishesAfterAddressing = [self.servant.loadPlate(dish, self.filter) for dish in result_set]

        final_df = pd.concat(dishesAfterAddressing, ignore_index=True)

        final_df = self.compensate(final_df, desired_signals, self.servant.getInvolvedSignals())

        for saver in data_savers:
            saver.save(final_df, desired_signals=desired_signals)

        return final_df

    def setFilter(self, filter):
        self.filter = filter
