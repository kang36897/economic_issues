# -*- coding: utf-8 -*-


import unittest
from unittest import TestCase
import pandas as pd
from os import path


class DualSignalTest(TestCase):

    def test_CJM622_CJM815(self):
        df = pd.read_csv(path.abspath("expected/CJM622+CJM815.csv"), header = 0)
        print df


if __name__ == '__main__':
    unittest.main()
