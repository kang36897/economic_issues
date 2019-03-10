# -*- coding: utf-8 -*-
from multiprocessing import Pool, Manager

from Chef import Chef
import pandas as pd

def filter(row):
    """
    the function used to filter the result DataFrame
    :param row:
    :return:
    """
    # if row["drawback_ratio"] > 30:
    #     return False

    return True
def wrapper(chef, st):
    return chef.handleOrder(st)

class Restaurant:

    def __init__(self, cpu_num, servant, balance):
        self.cpu_num = cpu_num
        self.p = Pool(cpu_num)
        self.m = Manager()
        self.servant = servant

        self.queue = self.m.Queue()
        # self.availableChefs = self.m.Queue(self.cpu_num)
        self.balance = balance

    def serveCustomer(self, desired_signals):
        taskSequence = self.servant.receiveOrders(desired_signals)

        for st in taskSequence:
            self.queue.put(st)

        # for i in range(self.cpu_num):
        #     self.availableChefs.put()

        async_result_set = []
        result_set = []
        while True:

            if len(async_result_set) < self.cpu_num:
                st = self.queue.get()
                chef = Chef(self.queue,
                                         names_of_signals=desired_signals,
                                         standard_deviation_of_signals= self.servant.getStandardDeviationOfSignals(),
                                         expected_return_of_signals = self.servant.getExpectedReturnOfSignals(),
                                         net_withdrawal_of_signals = self.servant.getNetWithdrawalOfSignals(),
                                         relation = self.servant.getRelationOfSignal(),
                                         balance = self.balance
                                         )

                temp = self.p.apply_async(wrapper, (chef, st))
                async_result_set.append(temp)

            else:

                ar = async_result_set.pop(0)

                dish = ar.get()
                # self.availableChefs.put(chef)

                result_set.append(self.servant.loadPlate(dish, filter))

            if self.queue.empty():
                break

        self.p.close()
        self.p.join()

        final_df = pd.concat([r.get() for r in async_result_set], ignore_index=True)
        final_df.to_csv("outputs/final_prediction.csv", encoding="utf-8", float_format="%.2f")