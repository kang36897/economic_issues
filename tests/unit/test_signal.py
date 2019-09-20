# -*- coding: utf-8 -*-
import unittest

from restaurant.Signal import Signal


class SignalTestCase(unittest.TestCase):
    def setUp(self):
        pass

    def test_len(self):
        min_lots = 0.01
        step = 0.01
        max_lots = 5
        expected = int((max_lots - min_lots) / step) + 1

        signal = Signal()
        self.assertEqual(expected, len(signal))


if __name__ == '__main__':
    unittest.main()
