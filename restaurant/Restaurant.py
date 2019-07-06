# -*- coding: utf-8 -*-
import itertools
from multiprocessing import Pool, Manager, Lock

from Chef import Chef
import pandas as pd
from os import path
from copy import copy

from restaurant.Sieve import Sieve


def wrapper(chef, st):
    return chef.handleOrder(st)




def collect_active_signals(row, full_signals):
    count = 0
    for item in full_signals:
        if row[item] != 0:
            count += 1

    return count

def compensate(df, desired_signals, full_signals):
    shared_columns = []

    for item in df.columns:
        if item not in desired_signals:
            shared_columns.append(item)

    for item in full_signals:
        if item not in desired_signals:
            df[item] = 0.0

    ideal_columns = copy(full_signals)
    ideal_columns.extend(shared_columns)

    df['active_num'] = df.apply(collect_active_signals, args=(full_signals,), axis=1)
    ideal_columns.insert(0, 'active_num')
    return df[ideal_columns]

def carryOut(task):
    st, chef_requirement, draftSieve, full_signals, data_savers = task
    names_of_signals, references_of_signals, standard_deviation_of_signals,\
    expected_return_of_signals, net_withdrawal_of_signals, relation, balance = chef_requirement

    sieve = None
    if draftSieve is not None:
        drawback_ratio, exp_return_ratio, sharp_ratio, pl_ratio, max_active_num = draftSieve
        sieve = Sieve(drawback_ratio=drawback_ratio, exp_return_ratio=exp_return_ratio,
                      sharp_ratio=sharp_ratio, pl_ratio=pl_ratio, max_active_num=max_active_num)

    chef = Chef(
        names_of_signals=names_of_signals,
        references_of_signals=references_of_signals,
        standard_deviation_of_signals=standard_deviation_of_signals,
        expected_return_of_signals=expected_return_of_signals,
        net_withdrawal_of_signals=net_withdrawal_of_signals,
        relation=relation,
        balance=balance
    )
    chef.setColumnsInRedefinedOrder(
        [u'balance', u'covariance', u'times', u'drawback', u'drawback%', u'exp_profit', u'exp_profit%',
         u'sharp%', u'pl%'])

    dish = chef.handleOrder(st)
    filtered_df = dish.loc[dish.apply(sieve.filter, axis=1), :]
    final_df = compensate(filtered_df, names_of_signals, full_signals)

    filePrefix = ['{}({}@{})'.format(st.no, (st.start / st.page_size), (st.record_size / st.page_size))]
    filePrefix.extend(names_of_signals)
    for saver in data_savers:
        saver.save(final_df, desired_signals= filePrefix)

    st.reportProgress()

    return True

class Restaurant:

    def __init__(self, cpu_num, servant, balance):
        self.cpu_num = cpu_num
        self.p = Pool(cpu_num)
        self.m = Manager()
        self.servant = servant


        self.balance = balance
        self.task_lock = Lock()

        self.sieve = None

    def getTaskLock(self):
        return self.task_lock



    def serveCustomer(self, desired_signals, data_savers):
        taskSequence = self.servant.receiveOrders(desired_signals)

        names_of_signals = desired_signals
        references_of_signals = self.servant.getReferencesOfSignals()
        standard_deviation_of_signals = self.servant.getStandardDeviationOfSignals()
        expected_return_of_signals = self.servant.getExpectedReturnOfSignals()
        net_withdrawal_of_signals = self.servant.getNetWithdrawalOfSignals()
        relation = self.servant.getRelationOfSignal()
        balance = self.balance

        full_signals = self.servant.getInvolvedSignals()

        self.p.map(carryOut, [(st, (names_of_signals, references_of_signals, standard_deviation_of_signals, expected_return_of_signals, net_withdrawal_of_signals, relation, balance), self.sieve, full_signals, data_savers) for st in itertools.chain(taskSequence)])

        self.p.close()
        self.p.join()

    def setFilter(self, draftSieve):
        self.sieve = draftSieve
