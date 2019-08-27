# -*- coding: utf-8 -*-
import unittest
from itertools import product
from os import path

import numpy as np
from restaurant.Cook import Cook, Condiment
from restaurant.SmallTask import SmallTask

from restaurant.Utils import compareListIgnoreOrder


class CookTest(unittest.TestCase):
    def test_collectTomato(self):
        hm = Cook()
        hm.collectTomato(path.abspath("resources/signals.xlsx"))


    def test_describeDishes(self):
        hm = Cook()

        condiment = Condiment()
        hm.collect(condiment)
        possible_times = {'a' : 11.00}
        dishes = hm.describeDishes(possible_times)
        expected = [ np.around( np.float64(x) , decimals=2) for x in np.arange(0,  10, step = np.float64(0.01))]
        self.assertEquals(expected, dishes['a'])

        possible_times = {'b' : 98}
        condiment = Condiment(maxlots= 50)
        hm.collect(condiment)
        dishes = hm.describeDishes(possible_times)
        expected = [np.around( np.float64(x) , decimals=2) for x in np.arange(0, 50, step=np.float64(0.01))]
        self.assertEquals(expected, dishes['b'])

        condiment = Condiment(minlots= 0.1)
        hm.collect(condiment)
        possible_times = {'c': 12}
        dishes = hm.describeDishes(possible_times)
        expected = [np.around( np.float64(x) , decimals=2) for x in np.arange(0.1, 10, step=np.float64(0.01))]
        self.assertEquals(expected, dishes['c'])

        condiment = Condiment(steplength=0.5)
        hm.collect(condiment)
        possible_times = {'d': 12}
        dishes = hm.describeDishes(possible_times)
        expected = [np.around(np.float64(x), decimals=2) for x in np.arange(0, 10, step=np.float64(0.5))]
        self.assertEquals(expected, dishes['d'])

        condiment = Condiment(maxlots = 1, minlots = 0.05, steplength = 0.1)
        hm.collect(condiment)
        possible_times = {'d': 2.15}
        dishes = hm.describeDishes(possible_times)
        expected = [np.around(np.float64(x), decimals=2) for x in np.arange(0.05, 1.01, step=np.float64(0.1))]
        self.assertEquals(expected, dishes['d'])

    def test_getSignalsInRelation_after_collectPotato(self):
        expect_columns = [u'CJM622', u'CJM815', u'CJM995', u'DEMOZ', u'DM0066', u'DM8034', u'USG']

        hm = Cook()
        hm.collectPotato(path.abspath("resources/relations.xlsx"))

        result_columns = hm.getSignalsInRelation()

        self.assertEqual(len(expect_columns), len(result_columns))

        for item in expect_columns:
            self.assertIn(item, result_columns)

    def test_getRelationOfSignal_after_collectPotato(self):
        columns = [u'CJM622', u'CJM815', u'CJM995', u'DEMOZ', u'DM0066', u'DM8034', u'USG']
        expected = [ t for t in product(columns, columns)]

        hm = Cook()
        hm.collectPotato(path.abspath("resources/relations.xlsx"))

        self.assertEqual(len(expected), len(hm.getRelationOfSignal().keys()))

        for item in expected:
            self.assertIn(item, hm.getRelationOfSignal().keys())

        l = hm.getRelationOfSignal()[(u'CJM622', u'CJM815')]
        r = hm.getRelationOfSignal()[( u'CJM815', u'CJM622')]
        self.assertEqual(l, r, "Their value should be equal whatever their orders are")


        for key in [(i, i) for i in columns]:
            self.assertEqual(1, hm.getRelationOfSignal()[key])

        self.assertEqual(np.float32(0.2406), hm.getRelationOfSignal()[(u'CJM815', u'DEMOZ')])

    def test_getRelationOfSignal(self):
        hm = Cook()

        self.assertIsNone(hm.getRelationOfSignal())

    def test_getSignalsInRelation(self):
        hm = Cook()
        self.assertEqual(None, hm.getSignalsInRelation())

    def test_checkSymmetryOfRelation(self):
        hm = Cook()
        df = hm.collectRelationMaterial(path.abspath("resources/relations.xlsx"))
        self.assertEqual(True, hm.checkSymmetryOfRelation(df))

    def test_checkSymmetryOfRelation(self):
        hm = Cook()
        df = hm.collectRelationMaterial(path.abspath("resources/relations_not_valid.xlsx"))
        self.assertEqual(False, hm.checkSymmetryOfRelation())

    def test_getInvolvedSignals(self):
        hm = Cook()
        self.assertIsNone(hm.getInvolvedSignals())

    def test_getInvolvedSignals_after_collectTomato(self):
        expected = [u'CJM622',u'CJM815', u'CJM995', u'DEMOZ', u'DM0066', u'DM8034', u'LYP', u'USG']

        hm = Cook()
        hm.collectTomato(path.abspath("resources/signals.xlsx"))

        self.assertEqual(len(expected), len(hm.getInvolvedSignals()))

    def test_getInvolvedSignalsInOrder_after_collectTomato(self):
        expected = [u'CJM622',u'CJM815', u'CJM995', u'DEMOZ', u'DM0066', u'DM8034', u'LYP', u'USG']

        hm = Cook()
        hm.collectTomato(path.abspath("resources/signals.xlsx"))

        self.assertItemsEqual(expected, hm.sortInvolvedSignals())

    def test_getReferencesOfSignals_after_collectTomato(self):
        expected = {
            'LYP': np.float64(0.1),
            'CJM622': np.float64(0.1),
            'DEMOZ': np.float64(0.1),
            'CJM815': np.float64(0.63),
            'CJM995': np.float64(1),
            'DM0066': np.float64(0.5),
            'USG': np.float64(0.02),
            'DM8034': np.float64(0.05)
        }

        hm = Cook()
        hm.collectTomato(path.abspath("resources/signals.xlsx"))

        for key, value in expected.items():
            self.assertEqual(expected[key], hm.getReferencesOfSignals()[key])


    def test_getAvailableSignals(self):

        hm = Cook()
        hm.collectPotato(path.abspath("resources/relations_less.xlsx"))

        self.assertEqual(None, hm.getAvailableSignals())

    def test_getAvailableSignals_after_collectTomato(self):
        expected = [u'CJM622', u'CJM815', u'CJM995', u'DEMOZ', u'DM0066', u'USG']

        hm = Cook()
        hm.collectPotato(path.abspath("resources/relations_less.xlsx"))
        hm.collectTomato(path.abspath("resources/signals.xlsx"))

        result = hm.getAvailableSignals()
        self.assertEqual(len(expected), len(result))

        for item in expected:
            self.assertIn(item, result)


    def test_getStandardDeviationOfSignals(self):

        expected_standard_deviation_of_signals = {
            u'CJM622': np.float64(1764.4569),
            u'CJM815': np.float64(3125.9093),
            u'CJM995': np.float64(32.0641),
            u'DEMOZ': np.float64(1088.2825),
            u'DM0066': np.float64(3087.8561),
            u'DM8034': np.float64(273.9382),
            u'LYP' : np.float64(9810.1376),
            u'USG' : np.float64(0.3118)
        }

        hm = Cook()
        hm.collectPotato(path.abspath("resources/relations_less.xlsx"))
        hm.collectTomato(path.abspath("resources/signals.xlsx"))

        for key, value in expected_standard_deviation_of_signals.items():
            self.assertEqual(value, hm.getStandardDeviationOfSignals()[key])

    def test_getExpectedReturnOfSignals(self):

        expected_expected_return_of_signals = {
            u'CJM622': np.float64(2163.80),
            u'CJM815': np.float64(543.3),

        }

        hm = Cook()
        hm.collectPotato(path.abspath("resources/relations_less.xlsx"))
        hm.collectTomato(path.abspath("resources/signals.xlsx"))

        for key, value in expected_expected_return_of_signals.items():
            self.assertEqual(value, hm.getExpectedReturnOfSignals()[key], msg="for {} not match".format(key))


    def test_getNetWithdrawalOfSignals(self):
        expected_net_withdrawal_of_signals = {
            u'CJM622': np.float64(-6124),
            u'CJM815': np.float64(-13207),
            u'CJM995': np.float64(-248.62),
            u'DEMOZ': np.float64(-7011.96),
            u'DM0066': np.float64(-10335.67),
            u'DM8034': np.float64(-5539.45),
            u'LYP': np.float64(-39243),
            u'USG': np.float64(-9203.98)
        }

        hm = Cook()
        hm.collectPotato(path.abspath("resources/relations_less.xlsx"))
        hm.collectTomato(path.abspath("resources/signals.xlsx"))

        for key, value in expected_net_withdrawal_of_signals.items():
            self.assertEqual(value, hm.getNetWithdrawalOfSignals()[key])


    def test_getPossibleTimes(self):

        expected_possible_times = {
            u'CJM622': np.float64(0.1),
            u'CJM815': np.float64(0.63),
            u'CJM995': np.float64(1.0),
            u'DEMOZ': np.float64(0.1),
            u'DM0066': np.float64(0.5),
            u'DM8034': np.float64(0.05),
            u'LYP': np.float64(0.2),
            u'USG': np.float64(0.02)
        }

        hm = Cook()
        hm.collectPotato(path.abspath("resources/relations_less.xlsx"))
        hm.collectTomato(path.abspath("resources/signals.xlsx"))

        for key, value in expected_possible_times.items():
            self.assertEqual(value, hm.getPossibleTimes()[key])

    def test_pickUrgentOrder(self):
        possible_times = {
            u'CJM622': np.float64(0.1),
            u'CJM815': np.float64(0.63),
            u'CJM995': np.float64(1.0),
            u'DEMOZ': np.float64(0.1),
            u'DM0066': np.float64(0.5),
            u'DM8034': np.float64(0.05),
            u'LYP': np.float64(0.2),
            u'USG': np.float64(0.02)
        }

        desired_signals = [u'CJM622', u'CJM995',u'DEMOZ',  u'LYP' ]
        hm = Cook()
        result = hm.pickUrgentOrder(possible_times, desired_signals)
        self.assertEqual((u'CJM995', np.float64(1.0)), result)


    def test_detailDish(self):
        possible_times = {u'CJM622': np.float64(0.1)}

        expected = {u'CJM622': [x for x in np.arange(0, np.float64(0.1), step= np.float64(0.01), dtype= np.float64)]}

        print expected

        hm = Cook()
        hm.collect(Condiment())
        print hm.describeDishes(possible_times)
        for key, value in expected.items():
            self.assertTrue(compareListIgnoreOrder(value, hm.describeDishes(possible_times)[key]))


    def test_receiveOrders(self):
        largest = [np.float64(0.0), np.float64(0.01), np.float64(0.02), np.float64(0.03), np.float64(0.04), np.float64(0.05)]
        possible_times = {
            u'CJM622': np.float64(0.01),
            u'CJM815': np.float64(0.05),
            u'DEMOZ':np.float64(0.02)
        }

        columns = [u'CJM622', u'CJM815', u'DEMOZ']
        hm = Cook()
        hm.setPossibleTimes(possible_times)
        result = hm.receiveOrders(columns)

        for i in range(len(largest)):
            self.assertEqual(SmallTask(i, columns,
                [[largest[i]], [np.float64(0.0), np.float64(0.01)],[np.float64(0.0), np.float64(0.01), np.float64(0.02)]], start=0),
                            result[i] )

if __name__ == '__main__':
    unittest.main()