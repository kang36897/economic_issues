# -*- coding: utf-8 -*-
import unittest
from Utils import  compareListIgnoreOrder

class UtilsTest(unittest.TestCase):

    def test_compareListIgnoreOrder(self):

        left = ['a', 'b', 'c']
        right = ['a', 'b', 'c']

        self.assertTrue(compareListIgnoreOrder(left, right))


    def test_compareListIgnoreOrder_disorder(self):

        left = ['a', 'b', 'c']
        right = [ 'b', 'a','c']

        self.assertTrue(compareListIgnoreOrder(left, right))

    def test_compareListIgnoreOrder_disorder(self):
        left = ['a', 'b', 'c']
        right = ['b', 'a']

        self.assertFalse(compareListIgnoreOrder(left, right))


    def test_compareListIgnoreOrder_deepWrong(self):
        left = [['a'], ['b'], ['c']]
        right = [['b'], ['a']]

        self.assertFalse(compareListIgnoreOrder(left, right))


    def test_compareListIgnoreOrder_deepRight(self):
        left = [['a'], ['b'], ['c']]
        right = [['b'], ['a'], ['c']]

        self.assertTrue(compareListIgnoreOrder(left, right))