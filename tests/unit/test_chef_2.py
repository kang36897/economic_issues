# -*- coding: utf-8 -*-
import unittest
from multiprocessing import Manager

import pandas as pd

from restaurant.Chef import Chef
from restaurant.SmallTask import SmallTask
from restaurant.Utils import compareListIgnoreOrder


class ChefTest2(unittest.TestCase):

    def test_sliceTomato(self):
        expected_df = pd.DataFrame(data=[[0, 0], [0, 2], [0, 4], [1, 0], [1, 2], [1, 4]], columns=['a', 'b'])
        print expected_df

        st = SmallTask(0, ['a', 'b'], [[0, 1], [0, 2, 4]], start=0, default_page_size=6)
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
        expected_task = SmallTask(0, ['a', 'b'], [[0, 1], [0, 2, 4]], start=3, default_page_size=3)
        self.assertEqual(expected_task, nt)

    def test_setColumnMapper(self):
        expected = {
            u'drawback%': u'drawback_ratio',
            u'exp_profit': u'exp_return',
            u'exp_profit%': u'exp_return_ratio',
            u'sharp%': u"sharp_ratio"
        }
        m = Manager()
        queue = m.Queue()

        chef = Chef(queue)
        chef.setColumnMapper(expected)

        for key, value in expected.items():
            self.assertEqual(value, chef.getColumnMapper()[key])


if __name__ == '__main__':
    unittest.main()
