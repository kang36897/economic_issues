# -*- coding: utf-8 -*-
import unittest
from multiprocessing import Manager
from unittest import TestCase

from restaurant.Chef import Chef
from restaurant.SmallTask import SmallTask


class ChefTest4(TestCase):

    def test_setColumnsInRedefinedOrder(self):
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

        st = SmallTask(0, ['a', 'b'], [[0.1], [0.2]], start=0, default_page_size=2)
        m = Manager()
        queue = m.Queue()

        chef = Chef(queue,
                    names_of_signals=names_of_signals,
                    standard_deviation_of_signals=standard_deviation_of_signals,
                    expected_return_of_signals=expected_return_of_signals,
                    net_withdrawal_of_signals=net_withdrawal_of_signals,
                    relation=relationship, balance=120)
        chef.setColumnsInRedefinedOrder([u'balance', u'corelation', u'times', u'drawback', u'drawback%',
                                         u'exp_profit', u'exp_profit%', u'sharp%'])
        self.assertItemsEqual([u'a', u'b', u'balance', u'corelation', u'times', u'drawback', u'drawback%',
                               u'exp_profit', u'exp_profit%', u'sharp%'],
                              chef.getColumnsInRedefinedOrder())


if __name__ == '__main__':
    unittest.main()
