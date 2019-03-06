# -*- coding: utf-8 -*-
import unittest

from SmallTask import SmallTask
class SmallTaskTest(unittest.TestCase):

    def test_isComplete_after_creation(self):

        task = SmallTask(0, ["A", 'B'], [[1], [0, 1]], default_page_size= 3)
        self.assertFalse(task.isComplete())



if __name__ == '__main__':
    unittest.main()