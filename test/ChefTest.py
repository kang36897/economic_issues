# -*- coding: utf-8 -*-
import unittest

from Chef import Chef, calculate_covariance , calculate_return, calculate_multiple, calculate_sharp_rate
from Cook import Cook
from SmallTask import SmallTask
from multiprocessing import Manager, Queue
import pandas as pd
from Utils import compareListIgnoreOrder
import math

def filter(row):
    """
    the function used to filter the result DataFrame
    :param row:
    :return:
    """
    # if row["drawback_ratio"] > 30:
    #     return False

    return True

class ChefTest(unittest.TestCase):

    def test_sliceTomato(self):
        expected_df = pd.DataFrame(data = [[0, 0], [0, 2], [0,4], [1, 0], [1, 2], [1, 4]], columns= ['a', 'b'])
        print expected_df

        st = SmallTask(0, ['a', 'b'], [[0, 1], [0, 2, 4]], start=0, default_page_size= 6)
        m = Manager()
        queue = m.Queue()

        chef = Chef(queue)
        df = chef.sliceTomato(st)

        self.assertEqual(len(st.column_names), len(df.columns))
        for item in st.column_names:
            self.assertIn(item, df.columns)

        self.assertTrue(expected_df.equals(df))
        self.assertTrue(queue.empty())

    def test_sliceTomato_with_one(self):
        expected_df = pd.DataFrame(data=[[0, 0], [0, 2], [0, 4]], columns=['a', 'b'])
        print expected_df

        st = SmallTask(0, ['a', 'b'], [[0, 1], [0, 2, 4]], start=0, default_page_size=3)
        m = Manager()
        queue = m.Queue()

        chef = Chef(queue)
        df = chef.sliceTomato(st)

        self.assertTrue(compareListIgnoreOrder(st.column_names, df.columns))
        for item in st.column_names:
            self.assertIn(item, df.columns)

        self.assertTrue(expected_df.equals(df))
        self.assertFalse(queue.empty())

        nt = queue.get()
        expected_task = SmallTask(0, ['a', 'b'], [[0, 1], [0, 2, 4]], start= 3,default_page_size=3 )
        self.assertEqual(expected_task, nt)

    def test_calculate_covariance(self):
        row = {"a":0.1,"b": 0.2}
        names_of_signals = ['a', 'b']
        standard_deviation_of_signals = {'a': 0.3, 'b' : 0.7}
        relationship = {('a', 'a'): 1, ('b', 'b'):1, ('a', 'b'): 0.5, ('b', 'a'): 0.5}

        expected = math.sqrt(math.pow(0.1 * 0.3, 2) + math.pow(0.2 * 0.7, 2) + 2 * (0.1 * 0.3) * (0.2 * 0.7) * 0.5)
        self.assertEqual(expected,calculate_covariance(row, names_of_signals, standard_deviation_of_signals, relationship))

    def test_calculate_covariance_3(self):
        row = {"a": 0.1, "b": 0.2, 'c': 0.3}
        names_of_signals = ['a', 'b', 'c']
        standard_deviation_of_signals = {'a': 0.3, 'b': 0.7, 'c': 0.5}
        relationship = {('a', 'a'): 1,
                        ('b', 'b'): 1,
                        ('c', 'c'): 1,
                        ('a', 'b'): 0.5,
                        ('b', 'a'): 0.5,
                        ('a', 'c') : 0.7,
                        ('c', 'a') : 0.7,
                        ('b', 'c') : 0.3,
                        ('c', 'b') : 0.3
                         }

        expected = math.sqrt(math.pow(0.1 * 0.3, 2) + math.pow(0.2 * 0.7, 2) + math.pow(0.3 * 0.5, 2)
                             + 2 * (0.1 * 0.3) * (0.2 * 0.7) * 0.5
                             + 2 * (0.1 * 0.3) * (0.3 * 0.5) * 0.7
                             + 2 * (0.2 * 0.7) * (0.3 * 0.5) * 0.3)
        self.assertEqual(expected,
                         calculate_covariance(row, names_of_signals, standard_deviation_of_signals, relationship))

    def test_calculate_return(self):
        names_of_signals = ['a', 'b']
        row = {"a": 0.1, "b": 0.2, 'c': 0.3}
        expected_return_of_signals = {'a': 0.21, 'b': 0.33, 'c': 0.47}

        expected = 0.1 * 0.21 + 0.2 * 0.33
        self.assertEqual(expected, calculate_return(row, names_of_signals, expected_return_of_signals))

    def test_calculate_multiple(self):
        expect = {'balance': 100, 'corelation': 30}
        self.assertEqual(100 / 30,calculate_multiple(expect) )

        expect2 = {'balance': 100, 'corelation': 0}
        self.assertEqual(0, calculate_multiple(expect2))

        expect2 = {'balance': 100, 'corelation': 12.0}
        self.assertEqual(100 / 12.0, calculate_multiple(expect2))

    def test_calculate_sharp_rate(self):
         expected = {"balance": 120, "corelation": 10.23, "exp_return": 20.12}
         self.assertEqual((20.12 * 12 - 120 * 0.05)/ 10.23 ,calculate_sharp_rate(expected))

    def test_stir(self):
        names_of_signals = ['a', 'b']
        expected_return_of_signals = {'a': 0.21, 'b': 0.33, 'c': 0.47}
        standard_deviation_of_signals = {'a': 0.3, 'b': 0.7, 'c': 0.5}
        net_withdrawal_of_signals = {'a': 0.35, 'b': 0.17, 'c': 0.25}
        relationship = {('a', 'a'): 1,
                        ('b', 'b'): 1,
                        ('c', 'c'): 1,
                        ('a', 'b'): 0.5,
                        ('b', 'a'): 0.5,
                        ('a', 'c'): 0.7,
                        ('c', 'a'): 0.7,
                        ('b', 'c'): 0.3,
                        ('c', 'b'): 0.3
                        }



        expected_df = pd.DataFrame(data = [[0.1, 0.2]], columns=['a', 'b'])
        expected_df["balance"] = 120

        expected_df["corelation"] = expected_df.apply(calculate_covariance, axis=1,
                                                  args=(
                                                  names_of_signals, standard_deviation_of_signals, relationship))


        expected_df["drawback"] = expected_df.apply(calculate_covariance, axis=1,
                                                      args=(
                                                          names_of_signals, net_withdrawal_of_signals,
                                                          relationship))
        expected_df["exp_return"] = expected_df.apply(calculate_return, axis=1,
                                                                args=(names_of_signals, expected_return_of_signals))

        expected_df["times"] = expected_df.apply(calculate_multiple, axis=1)

        expected_df["drawback_ratio"] = expected_df["drawback"] * 100 / expected_df["balance"]
        expected_df["exp_return_ratio"] = expected_df["exp_return"] * 100 / expected_df["balance"]

        expected_df = expected_df.round({"drawback_ratio": 2, "exp_return_ratio": 2})
        expected_df["sharp_ratio"] = expected_df.apply(calculate_sharp_rate, axis=1)

        print expected_df

        st = SmallTask(0, ['a', 'b'], [[0.1], [0.2]], start=0, default_page_size=2)
        m = Manager()
        queue = m.Queue()

        chef = Chef(queue,
                    names_of_signals= names_of_signals,
                    standard_deviation_of_signals = standard_deviation_of_signals,
                    expected_return_of_signals = expected_return_of_signals,
                    net_withdrawal_of_signals = net_withdrawal_of_signals,
                    relation = relationship, balance= 120)
        chef.sliceTomato(st)
        df = chef.stir()
        print df
        self.assertTrue(expected_df.equals(df))

    def test_loadPlate(self):
        names_of_signals = ['a', 'b']
        expected_return_of_signals = {'a': 0.21, 'b': 0.33, 'c': 0.47}
        standard_deviation_of_signals = {'a': 0.3, 'b': 0.7, 'c': 0.5}
        net_withdrawal_of_signals = {'a': 0.35, 'b': 0.17, 'c': 0.25}
        relationship = {('a', 'a'): 1,
                        ('b', 'b'): 1,
                        ('c', 'c'): 1,
                        ('a', 'b'): 0.5,
                        ('b', 'a'): 0.5,
                        ('a', 'c'): 0.7,
                        ('c', 'a'): 0.7,
                        ('b', 'c'): 0.3,
                        ('c', 'b'): 0.3
                        }

        expected_df = pd.DataFrame(data=[[0.1, 0.2]], columns=['a', 'b'])
        expected_df["balance"] = 120

        expected_df["corelation"] = expected_df.apply(calculate_covariance, axis=1,
                                                      args=(
                                                          names_of_signals, standard_deviation_of_signals,
                                                          relationship))

        expected_df["drawback"] = expected_df.apply(calculate_covariance, axis=1,
                                                    args=(
                                                        names_of_signals, net_withdrawal_of_signals,
                                                        relationship))
        expected_df["exp_return"] = expected_df.apply(calculate_return, axis=1,
                                                      args=(names_of_signals, expected_return_of_signals))

        expected_df["times"] = expected_df.apply(calculate_multiple, axis=1)

        expected_df["drawback_ratio"] = expected_df["drawback"] * 100 / expected_df["balance"]
        expected_df["exp_return_ratio"] = expected_df["exp_return"] * 100 / expected_df["balance"]

        expected_df = expected_df.round({"drawback_ratio": 2, "exp_return_ratio": 2})
        expected_df["sharp_ratio"] = expected_df.apply(calculate_sharp_rate, axis=1)

        print expected_df

        st = SmallTask(0, ['a', 'b'], [[0.1], [0.2]], start=0, default_page_size=2)
        m = Manager()
        queue = m.Queue()

        chef = Chef(queue,
                    names_of_signals=names_of_signals,
                    standard_deviation_of_signals=standard_deviation_of_signals,
                    expected_return_of_signals=expected_return_of_signals,
                    net_withdrawal_of_signals=net_withdrawal_of_signals,
                    relation=relationship, balance=120)

        dish = chef.handleOrder(st)
        c = Cook()
        self.assertTrue(expected_df.equals(c.loadPlate(dish, filter)))

        self.assertFalse(expected_df.equals(c.loadPlate(dish, lambda row: row["drawback_ratio"] > 30)))


if __name__ == '__main__':
    unittest.main()


