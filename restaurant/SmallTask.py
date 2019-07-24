# -*- coding: utf-8 -*-
import math
from itertools import islice
from itertools import product

from Utils import compareListIgnoreOrder


def calculate_record_size(columns_seeds):
    total = 1

    for s in columns_seeds:
        total *= len(s)

    return total


class SmallTask:
    def __init__(self, no, column_names, column_seeds, start=0, default_page_size=100 *1000):
        self.no = no
        self.column_names = column_names
        self.column_seeds = column_seeds

        self.record_size = calculate_record_size(column_seeds)
        self.page_size = min(default_page_size, self.record_size)

        self.start = start
        self.isSeed = True if start == 0 else False
        self.stop = self.start + self.page_size
        self.toNext = 0
        self.is_complete = False


    def __len__(self):
        return int(math.ceil(self.record_size / self.page_size))

    def generateNextTask(self):

        nextTask = SmallTask(self.no, self.column_names, self.column_seeds, start=self.toNext,
                             default_page_size=self.page_size)

        self.toNext = self.toNext + self.page_size
        return nextTask;

    def __iter__(self):
        return self;


    def next(self):
        if self.isDone():
            raise StopIteration;


        return self.generateNextTask()

    def generateDataSource(self):
        return product(*self.column_seeds)

    def generateFrame(self):
        frame = islice(self.generateDataSource(), self.start, self.stop)
        self.is_complete = True
        self.isSeed = False

        return frame

    def isDone(self):
        return self.toNext >= self.record_size

    def isComplete(self):
        return self.is_complete

    def reportProgress(self):
        return "Task {} -> finished : {}, current progress: {}%".format(self.no, self.stop,
                                (100 if self.stop >= self.record_size else int(self.stop * 100.0 / self.record_size)))

    def __eq__(self, other):
        if not isinstance(other, SmallTask):
            return False

        if not compareListIgnoreOrder(self.column_names, other.column_names):
            return False

        if not compareListIgnoreOrder(self.column_seeds, other.column_seeds):
            return False

        if self.no != other.no or self.start != other.start or self.record_size != other.record_size or self.page_size != other.page_size:
            return False

        return True
