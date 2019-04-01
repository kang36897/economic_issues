# -*- coding: utf-8 -*-
import unittest
from unittest import TestCase
from restaurant.Chain import  Chain
class ChainTest(TestCase):

    def test_iteratePossiblePackage(self):
        input = ['a', 'b', 'c']
        expected = [
            ['a'],
            ['b'],
            ['c'],
            ['a', 'b'],
            ['a', 'c'],
            ['b', 'c'],
            ['a', 'b', 'c']
        ]

        chain = Chain(None, 0, 100)
        result = chain.iteratePossiblePackage(input)
        print result

        self.assertItemsEqual(expected,result)


if __name__ == '__main__':
    unittest.main()