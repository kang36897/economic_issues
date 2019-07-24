# -*- coding: utf-8 -*-
import unittest

from restaurant.SmallTask import SmallTask

class SmallTaskTest(unittest.TestCase):


    def test_iterables(self):
        task = SmallTask(0, ["A", 'B', 'C', 'D'], [[1], [0, 1], [0], [0, 1, 2]], default_page_size=3)

        iterator = iter(task)
        self.assertEquals(iterator, task)

        firstBatch = next(iterator)
        expected = [(1, 0, 0, 0), (1, 0, 0, 1), (1, 0, 0, 2)]

        self.assertEquals(expected, [item for item in firstBatch.generateFrame()])
        print firstBatch.reportProgress()

        secondBatch = next(iterator)
        expected = [(1, 1, 0, 0), (1, 1, 0, 1), (1, 1, 0, 2)]
        self.assertEquals(expected, [item for item in secondBatch.generateFrame()])
        print secondBatch.reportProgress()

        self.assertRaises(StopIteration, next, iterator)

    def test_isDone_after_creation(self):

        task = SmallTask(0, ["A", 'B', 'C', 'D'], [[1], [0, 1], [0], [0, 1, 2]], default_page_size= 3)
        self.assertFalse(task.isDone(), msg="isDone() should be false after task is created firstly")


    def test_generateDataSource(self):
        task = SmallTask(0, ["A", 'B'], [[1], [0, 1]], default_page_size= 3)
        it = task.generateDataSource()
        self.assertEqual(it.next(),(1,0))
        self.assertEqual(it.next(),(1,1))


    def test_generateFrame(self):
        task = SmallTask(0, ["A", 'B'], [[0,1], [0, 1, 2]], default_page_size= 3)
        it = task.generateFrame()

        result = list(it)
        self.assertEqual(3, len(result), msg="there should be 3 items in one frame")
        self.assertListEqual([(0, 0), (0, 1), (0, 2)], result)


    def test_generateNextTask(self):
        task = SmallTask(0, ["A", 'B'], [[0, 1], [0, 1, 2]], default_page_size=3)
        iterator = iter(task)
        next(task)

        nextTask = next(task)

        it = nextTask.generateFrame()
        result = list(it)

        self.assertEqual(3, len(result), msg="there should be 3 items in one frame")
        self.assertListEqual([(1, 0), (1, 1), (1, 2)], result)


    def test_reportProgress(self):
        pageSize = 3;
        task = SmallTask(0, ["A", 'B'], [[0, 1], [0, 1, 2]], default_page_size=pageSize)


        self.assertEqual("Task {} -> finished : {}, current progress: {}%".format(0, pageSize, 50),
                         task.reportProgress())
        iterator = iter(task)
        next(iterator)

        nextTask = next(iterator)
        self.assertEqual("Task {} -> finished : {}, current progress: {}%".format(0, pageSize * 2, 100),
                         nextTask.reportProgress())

    def test_equals(self):
        task = SmallTask(0, ["A", 'B'], [[0, 1], [0, 1, 2]], default_page_size=3)
        task.generateFrame()

        ntask = task.generateNextTask()

        self.assertNotEqual(task, ntask)

    def test_equals_new(self):
        t1 = SmallTask(0, ["A", 'B'], [[0, 1], [0, 1, 2]], default_page_size=3)

        t2 = SmallTask(0, ["A", 'B'], [[0, 1], [0, 1, 2]], default_page_size=3)

        self.assertEqual(t1, t2)



if __name__ == '__main__':
    unittest.main()