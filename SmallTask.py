# -*- coding: utf-8 -*-


def calculate_record_size(columns_seeds):
    total = 1

    for s in columns_seeds:
        total *= len(s)

    return total


class SmallTask:
    def __init__(self, no, column_names, column_seeds, default_page_size=100 * 1000):
        self.no = no
        self.column_names = column_names
        self.column_seeds = column_seeds

        self.record_size = calculate_record_size(column_seeds)
        self.page_size = default_page_size
        self.start = 0
        self.stop = self.start + default_page_size

    def update(self):
        self.start = self.stop
        self.stop = self.start + self.page_size

    def isComplete(self):
        return self.start > self.record_size

