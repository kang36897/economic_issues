# -*- coding: utf-8 -*-
import math
import unittest

from restaurant.Chef import calculate_covariance, calculate_multiple, calculate_return, calculate_sharp_rate


class ChefTest1(unittest.TestCase):
    def test_calculate_covariance(self):
        row = {"a": 0.1, "b": 0.2}
        names_of_signals = ['a', 'b']
        standard_deviation_of_signals = {'a': 0.3, 'b': 0.7}
        relationship = {('a', 'a'): 1, ('b', 'b'): 1, ('a', 'b'): 0.5, ('b', 'a'): 0.5}
        references_of_signals = {'a': 0.1, 'b': 0.1}

        expected = math.sqrt(math.pow((0.1 / 0.1) * 0.3, 2) + math.pow((0.2 / 0.1) * 0.7, 2) + 2 * (0.1 / 0.1) * 0.3 * (
                0.2 / 0.1) * 0.7 * 0.5)
        self.assertEqual(expected, calculate_covariance(row, names_of_signals, references_of_signals,
                                                        standard_deviation_of_signals, relationship))

    def test_calculate_covariance_1(self):
        row = {"a": 0.1, "b": 0.2}
        names_of_signals = ['a']
        standard_deviation_of_signals = {'a': 0.3, 'b': 0.7}
        relationship = {('a', 'a'): 1, ('b', 'b'): 1, ('a', 'b'): 0.5, ('b', 'a'): 0.5}
        references_of_signals = {'a': 0.01, 'b': 0.1}

        expected = math.sqrt(math.pow((0.1 / 0.01) * 0.3, 2))
        self.assertEqual(expected,
                         calculate_covariance(row, names_of_signals, references_of_signals,
                                              standard_deviation_of_signals, relationship))

    def test_calculate_covariance_3(self):
        row = {"a": 0.1, "b": 0.2, 'c': 0.3}
        names_of_signals = ['a', 'b', 'c']
        standard_deviation_of_signals = {'a': 0.3, 'b': 0.7, 'c': 0.5}
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
        references_of_signals = {'a': 0.01, 'b': 0.1, 'c': 0.02}

        expected = math.sqrt(
            math.pow((0.1 / 0.01) * 0.3, 2) + math.pow((0.2 / 0.1) * 0.7, 2) + math.pow((0.3 / 0.02) * 0.5, 2)
            + 2 * ((0.1 / 0.01) * 0.3) * ((0.2 / 0.1) * 0.7) * 0.5
            + 2 * ((0.1 / 0.01) * 0.3) * ((0.3 / 0.02) * 0.5) * 0.7
            + 2 * ((0.2 / 0.1) * 0.7) * ((0.3 / 0.02) * 0.5) * 0.3)
        self.assertEqual(expected,
                         calculate_covariance(row, names_of_signals, references_of_signals,
                                              standard_deviation_of_signals, relationship))

    def test_calculate_return(self):
        names_of_signals = ['a', 'b']
        row = {"a": 0.1, "b": 0.2, 'c': 0.3}
        expected_return_of_signals = {'a': 0.21, 'b': 0.33, 'c': 0.47}
        references_of_signals = {'a': 0.01, 'b': 0.1, 'c': 0.02}

        expected = 0.1 * 0.21 / 0.01 + 0.2 * 0.33 / 0.1
        self.assertEqual(expected, calculate_return(row, names_of_signals, references_of_signals, expected_return_of_signals))

    def test_calculate_multiple(self):
        expect = {'balance': 100, 'corelation': 30}
        self.assertEqual(100 / 30, calculate_multiple(expect))

        expect2 = {'balance': 100, 'corelation': 0}
        self.assertEqual(0, calculate_multiple(expect2))

        expect2 = {'balance': 100, 'corelation': 12.0}
        self.assertEqual(100 / 12.0, calculate_multiple(expect2))

    def test_calculate_sharp_rate(self):
        expected = {"balance": 120, "corelation": 10.23, "exp_profit": 20.12}
        self.assertEqual((20.12 * 12 - 120 * 0.05) / 10.23, calculate_sharp_rate(expected))


if __name__ == '__main__':
    unittest.main()
