# -*- coding: utf-8 -*-
import itertools
from copy import copy
from multiprocessing import Pool, Manager, Lock

from Chef import Chef
from restaurant.Sieve import Sieve


class TaskWrapper:
    def __init__(self, task_cores, names_of_signals, references_of_signals, standard_deviation_of_signals,
                 expected_return_of_signals,net_withdrawal_of_signals, relation, balance,  draftSieve,
                 full_signals,data_savers):
        self.task_cores = task_cores
        self.names_of_signals = names_of_signals
        self.references_of_signals = references_of_signals
        self.standard_deviation_of_signals = standard_deviation_of_signals
        self.expected_return_of_signals = expected_return_of_signals
        self.net_withdrawal_of_signals = net_withdrawal_of_signals
        self.relation = relation
        self.balance = balance
        self.draftSieve = draftSieve
        self.full_signals = full_signals
        self.data_savers = data_savers


    def getSize(self):
        return self.task_cores.getSize()


    def __iter__(self):

        for task in self.task_cores:
            yield (task, (
        self.names_of_signals, self.references_of_signals, self.standard_deviation_of_signals, self.expected_return_of_signals,
        self.net_withdrawal_of_signals, self.relation, self.balance), self.draftSieve, self.full_signals, self.data_savers)



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
    names_of_signals, references_of_signals, standard_deviation_of_signals, \
    expected_return_of_signals, net_withdrawal_of_signals, relation, balance = chef_requirement

    sieve = Sieve()
    if draftSieve is not None:
        drawback_ratio, exp_return_ratio, sharp_ratio, pl_ratio, max_active_num, poison_mushroom = draftSieve
        sieve = Sieve(drawback_ratio=drawback_ratio, exp_return_ratio=exp_return_ratio,
                      sharp_ratio=sharp_ratio, pl_ratio=pl_ratio, max_active_num=max_active_num, poison_mushroom = poison_mushroom)


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
    row, column = filtered_df.shape

    if row == 0:
        print 'Task {} at ({}/{}) without no answer '.format(st.no, (st.start / st.page_size), (st.record_size / st.page_size))
        print st.reportProgress()
        return True

    final_df = compensate(filtered_df, names_of_signals, full_signals)

    filePrefix = ['{}({}@{})'.format(st.no, (st.start / st.page_size), (st.record_size / st.page_size))]
    filePrefix.extend(names_of_signals)
    for saver in data_savers:
        saver.save(final_df, desired_signals=filePrefix)

    print st.reportProgress()

    return True


class Restaurant:

    def __init__(self, cpu_num, servant, balance):
        self.cpu_num = cpu_num
        self.p = Pool(cpu_num)
        self.m = Manager()
        self.servant = servant

        self.balance = balance
        self.task_lock = Lock()

        self.draftSieve = None

    def getTaskLock(self):
        return self.task_lock

    def serveCustomer(self, desired_signals, data_savers):

        names_of_signals = desired_signals
        references_of_signals = self.servant.getReferencesOfSignals()
        standard_deviation_of_signals = self.servant.getStandardDeviationOfSignals()
        expected_return_of_signals = self.servant.getExpectedReturnOfSignals()
        net_withdrawal_of_signals = self.servant.getNetWithdrawalOfSignals()
        relation = self.servant.getRelationOfSignal()
        balance = self.balance

        full_signals = desired_signals

        start = 0
        record_in_batch = 100 * 1000

        task_sequence = TaskWrapper(self.servant.receiveOrders(desired_signals), names_of_signals,
                                         references_of_signals, standard_deviation_of_signals, expected_return_of_signals,
        net_withdrawal_of_signals, relation, balance, self.draftSieve, full_signals, data_savers)

        while start < task_sequence.getSize():
            self.p.map(carryOut, itertools.islice(task_sequence, start, start + record_in_batch) , chunksize = 10)
            start += record_in_batch


        self.p.close()
        self.p.join()


    def setFilter(self, draftSieve):
        self.draftSieve = draftSieve


