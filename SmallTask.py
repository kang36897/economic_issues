# -*- coding: utf-8 -*-
from itertools import product
from itertools import islice
from Utils import compareListIgnoreOrder
import math

def calculate_record_size(columns_seeds):
    total = 1

    for s in columns_seeds:
        total *= len(s)

    return total


class SmallTask:
    def __init__(self, no, column_names, column_seeds, start=0, default_page_size=100 * 1000):
        self.no = no
        self.column_names = column_names
        self.column_seeds = column_seeds

        self.record_size = calculate_record_size(column_seeds)
        if self.record_size < 1000:
            self.page_size = self.record_size
        else:
            self.page_size = min(default_page_size, self.page_size)

        self.start = start
        self.stop = self.start +  self.page_size
        self.is_complete = False

    def generateNextTask(self):

        return SmallTask(self.no, self.column_names, self.column_seeds, start=self.stop,
                         default_page_size=self.page_size)

    def generateDataSource(self):
        return product(*self.column_seeds)

    def generateFrame(self):
        frame = islice(self.generateDataSource(), self.start, self.stop)
        self.is_complete = True
        return frame

    def isDone(self):
        return self.is_complete and self.stop >= self.record_size

    def isComplete(self):
        return self.is_complete

    def reportProgress(self):
        return "Task {} -> finished : {}, current progress: {}%".format(self.no, self.stop,
                                                                        int(self.stop * 100.0 / self.record_size))

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