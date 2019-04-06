# -*- coding: utf-8 -*-
import unittest
from copy import copy
from multiprocessing import Manager
from unittest import TestCase

import pandas as pd

from restaurant.Chef import Chef, calculate_covariance, calculate_multiple, calculate_return, calculate_sharp_rate
from restaurant.SmallTask import SmallTask
from restaurant.Cook import Cook
from restaurant.Sieve import Sieve


class ChefTest3(TestCase):

    def setUp(self):
        self.names_of_signals = ['a', 'b']
        self.expected_return_of_signals = {'a': 0.21, 'b': 0.33, 'c': 0.47}
        self.standard_deviation_of_signals = {'a': 0.3, 'b': 0.7, 'c': 0.5}
        self.net_withdrawal_of_signals = {'a': 0.35, 'b': 0.17, 'c': 0.25}
        self.relationship = {('a', 'a'): 1,
                             ('b', 'b'): 1,
                             ('c', 'c'): 1,
                             ('a', 'b'): 0.5,
                             ('b', 'a'): 0.5,
                             ('a', 'c'): 0.7,
                             ('c', 'a'): 0.7,
                             ('b', 'c'): 0.3,
                             ('c', 'b'): 0.3
                             }

        self.references_of_signals = {'a': 0.01, 'b': 0.1, 'c': 0.02}
        self.expected_df = self.create_expected_df()

    def create_expected_df(self):
        expected_df = pd.DataFrame(data=[[0.1, 0.2]], columns=self.names_of_signals)
        expected_df["balance"] = 120

        expected_df["corelation"] = expected_df.apply(calculate_covariance, axis=1,
                                                      args=(
                                                          self.names_of_signals, self.references_of_signals,
                                                          self.standard_deviation_of_signals, self.relationship))

        expected_df["drawback"] = expected_df.apply(calculate_covariance, axis=1,
                                                    args=(
                                                        self.names_of_signals, self.references_of_signals,
                                                        self.net_withdrawal_of_signals, self.relationship))
        expected_df["exp_profit"] = expected_df.apply(calculate_return, axis=1,
                                                      args=(self.names_of_signals, self.expected_return_of_signals))

        expected_df["times"] = expected_df.apply(calculate_multiple, axis=1)

        expected_df["drawback%"] = expected_df["drawback"] * 100 / expected_df["balance"]
        expected_df["exp_profit%"] = expected_df["exp_profit"] * 100 / expected_df["balance"]

        expected_df = expected_df.round({"drawback%": 2, "exp_profit%": 2})
        expected_df["sharp%"] = expected_df.apply(calculate_sharp_rate, axis=1)

        expected_df['pl%'] = (expected_df['exp_profit'] / expected_df['drawback']) * 100
        print expected_df
        print expected_df.columns
        return expected_df

    def test_stir(self):
        # u'balance',u'corelation',u'times', u'drawback', u'drawback%', u'exp_profit', u'exp_profit%',u'sharp%'
        st = SmallTask(0, ['a', 'b'], [[0.1], [0.2]], start=0, default_page_size=2)
        m = Manager()
        queue = m.Queue()

        chef = Chef(queue,
                    names_of_signals=self.names_of_signals,
                    references_of_signals=self.references_of_signals,
                    standard_deviation_of_signals=self.standard_deviation_of_signals,
                    expected_return_of_signals=self.expected_return_of_signals,
                    net_withdrawal_of_signals=self.net_withdrawal_of_signals,
                    relation=self.relationship, balance=120)
        chef.sliceTomato(st)
        df = chef.stir()
        print df
        print df.columns
        self.assertTrue(self.expected_df.equals(df))

    def test_redefineColumns(self):
        self.expected_df.rename(mapper={
            u'exp_profit': u'exp_return',
            u'drawback%': u'drawback_ratio',
            u'exp_profit%': u'exp_return_ratio',
            u'sharp%': u"sharp_ratio"
        }, inplace=True, axis=1);

        newColumns = copy(self.names_of_signals)
        newColumns.extend(
            [u'balance', u'corelation', u'times', u'drawback', u'drawback_ratio', u'exp_return', u'exp_return_ratio',
             u"sharp_ratio"])
        self.expected_df = self.expected_df[newColumns]

        print "aftet change columns:"
        print self.expected_df

        st = SmallTask(0, ['a', 'b'], [[0.1], [0.2]], start=0, default_page_size=2)
        m = Manager()
        queue = m.Queue()

        chef = Chef(queue,
                    names_of_signals=self.names_of_signals,
                    references_of_signals=self.references_of_signals,
                    standard_deviation_of_signals=self.standard_deviation_of_signals,
                    expected_return_of_signals=self.expected_return_of_signals,
                    net_withdrawal_of_signals=self.net_withdrawal_of_signals,
                    relation=self.relationship, balance=120)

        chef.sliceTomato(st)
        chef.stir()
        df = chef.redefineColumns(
            newColumns,
            mapper={
                u'drawback%': u'drawback_ratio',
                u'exp_profit': u'exp_return',
                u'exp_profit%': u'exp_return_ratio',
                u'sharp%': u"sharp_ratio"
            })
        print df
        self.assertTrue(self.expected_df.equals(df))


    def test_loadPlate(self):

        st = SmallTask(0, ['a', 'b'], [[0.1], [0.2]], start=0, default_page_size=2)
        m = Manager()
        queue = m.Queue()

        chef = Chef(queue,
                    names_of_signals=self.names_of_signals,
                    references_of_signals=self.references_of_signals,
                    standard_deviation_of_signals=self.standard_deviation_of_signals,
                    expected_return_of_signals=self.expected_return_of_signals,
                    net_withdrawal_of_signals=self.net_withdrawal_of_signals,
                    relation=self.relationship, balance=120)

        dish = chef.handleOrder(st)
        c = Cook()
        sieve = Sieve()
        self.assertTrue(self.expected_df.equals(c.loadPlate(dish, sieve.filter)))
        self.assertFalse(self.expected_df.equals(c.loadPlate(dish, lambda row: row["drawback%"] > 30)))


if __name__ == '__main__':
    unittest.main()
