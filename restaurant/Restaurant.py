# -*- coding: utf-8 -*-
from multiprocessing import Pool, Manager

from Chef import Chef
import pandas as pd
from os import path

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
        self.balance = balance

    def serveCustomer(self, desired_signals, delivery_path = None, ):
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
                result_set.append(dish)
            if self.queue.empty():
                break

        self.p.close()
        self.p.join()

        result_set.extend([r.get() for r in async_result_set])

        dishesAfterAddressing = [self.servant.loadPlate(dish, filter) for dish in result_set]

        final_df = pd.concat(dishesAfterAddressing, ignore_index=True)

        if delivery_path is not None:
            final_df.to_csv(path.join(delivery_path, "{}.csv".format("+".join(desired_signals))), encoding="utf-8", float_format="%.2f")

        return final_df